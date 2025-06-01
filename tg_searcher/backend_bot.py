import html
from datetime import datetime
from io import BytesIO
from typing import Optional, List, Set, Dict, Union

import telethon.errors.rpcerrorlist
from telethon import events
from telethon.tl.patched import Message as TgMessage
from telethon.tl.types import (
    User,
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    PeerChannel
)

from .indexer import Indexer, IndexMsg
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .model import TelegramMedia, TelegramMessage, TelegramUser
from .session import ClientSession
from .aws import AWSClient, AWSConfig


class BackendBotConfig:
    def __init__(self, **kw):
        self.monitor_all = kw.get('monitor_all', False)
        self.excluded_chats: Set[int] = set(get_share_id(chat_id)
                                            for chat_id in kw.get('exclude_chats', []))
        self.cloudstorage = kw.get('cloudstorage', False)


class BackendBot:
    def __init__(self, common_cfg: CommonBotConfig, cfg: BackendBotConfig, cloud_client: AWSClient,
                 session: ClientSession, clean_db: bool, backend_id: str):
        self.id: str = backend_id
        self.session = session

        self._logger = get_logger(f'bot-backend:{backend_id}')
        self._cfg = cfg
        if clean_db:
            self._logger.info(f'Index will be cleaned')
        self._indexer: Indexer = Indexer(common_cfg.index_dir / backend_id, clean_db)

        # on startup, all indexed chats are added to monitor list
        self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
        self.excluded_chats = cfg.excluded_chats
        self.newest_msg: Dict[int, IndexMsg] = dict()

        # cloud storage
        self._cloud_client = cloud_client if cfg.cloudstorage else None

    async def start(self):
        self._logger.info(f'Init backend bot')

        for chat_id in self.monitored_chats:
            try:
                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Ready to monitor "{chat_name}" ({chat_id})')
            except Exception as e:
                self._logger.error(f'exception on get monitored chat (id={chat_id}): {e}')
                self.monitored_chats.remove(chat_id)
                self._indexer.ix.delete_by_term('chat_id', chat_id)
                self._logger.error(f'remove chat (id={chat_id}) from monitor list and clear its index')

        self._register_hooks()

    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int):
        return self._indexer.search(q, in_chats, page_len, page_num)

    def rand_msg(self) -> IndexMsg:
        return self._indexer.retrieve_random_document()

    def is_empty(self, chat_id=None):
        if chat_id is not None:
            with self._indexer.ix.searcher() as searcher:
                return not any(True for _ in searcher.document_numbers(chat_id=str(chat_id)))
        else:
            return self._indexer.ix.is_empty()

    async def download_history(
            self,
            chat_id: int,
            min_id: int,
            max_id: int,
            cloud: bool = False,
            call_back=None,
            skip_indexing: bool = False,
            skip_existing: bool = True
    ):
        share_id = get_share_id(chat_id)
        self._logger.info(f'Downloading history from {share_id} ({min_id=}, {max_id=})')
        self.monitored_chats.add(share_id)
        msg_list = []
        async for tg_message in self.session.iter_messages(chat_id, min_id=min_id, max_id=max_id):
            if cloud:
                await self.cloud_upload_message(tg_message, skip_existing)
            if msg_text := self._extract_text(tg_message) and not skip_indexing:
                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                sender = await self._get_sender_name(tg_message)
                msg = IndexMsg(
                    content=msg_text,
                    url=url,
                    chat_id=chat_id,
                    post_time=datetime.fromtimestamp(tg_message.date.timestamp()),
                    sender=sender,
                )
                msg_list.append(msg)
            if call_back:
                await call_back(tg_message.id)
        self._logger.info(f'fetching history from {share_id} complete, start writing index')
        writer = self._indexer.ix.writer()
        for msg in msg_list:
            self._indexer.add_document(msg, writer)
            self.newest_msg[share_id] = msg
        writer.commit()
        self._logger.info(f'write index commit ok')

    def clear(self, chat_ids: Optional[List[int]] = None):
        if chat_ids is not None:
            for chat_id in chat_ids:
                with self._indexer.ix.writer() as w:
                    w.delete_by_term('chat_id', str(chat_id))
            for chat_id in chat_ids:
                self.monitored_chats.remove(chat_id)
        else:
            self._indexer.clear()
            self.monitored_chats.clear()

    async def find_chat_id(self, q: str) -> List[int]:
        return await self.session.find_chat_id(q)

    async def get_index_status(self, length_limit: int = 4000):
        # TODO: add session and frontend name
        cur_len = 0
        sb = [  # string builder
            f'后端 "{self.id}"（session: "{self.session.name}"）总消息数: <b>{self._indexer.ix.doc_count()}</b>\n\n'
        ]
        overflow_msg = f'\n\n由于 Telegram 消息长度限制，部分对话的统计信息没有展示'

        def append_msg(msg_list: List[str]):  # return whether overflow
            nonlocal cur_len, sb
            total_len = sum(len(msg) for msg in msg_list)
            if cur_len + total_len > length_limit - len(overflow_msg):
                return True
            else:
                cur_len += total_len
                for msg in msg_list:
                    sb.append(msg)
                    return False

        if self._cfg.monitor_all:
            append_msg([f'{len(self.excluded_chats)} 个对话被禁止索引\n'])
            for chat_id in self.excluded_chats:
                append_msg([f'- {await self.format_dialog_html(chat_id)}\n'])
            sb.append('\n')

        append_msg([f'总计 {len(self.monitored_chats)} 个对话被加入了索引：\n'])
        for chat_id in self.monitored_chats:
            msg_for_chat = []
            num = self._indexer.count_by_query(chat_id=str(chat_id))
            msg_for_chat.append(f'- {await self.format_dialog_html(chat_id)} 共 {num} 条消息\n')
            if newest_msg := self.newest_msg.get(chat_id, None):
                msg_for_chat.append(f'  最新消息：<a href="{newest_msg.url}">{brief_content(newest_msg.content)}</a>\n')
            if append_msg(msg_for_chat):
                # if overflow
                sb.append(overflow_msg)
                break

        return ''.join(sb)

    async def translate_chat_id(self, chat_id: int) -> str:
        try:
            return await self.session.translate_chat_id(chat_id)
        except telethon.errors.rpcerrorlist.ChannelPrivateError:
            return '[无法获取名称]'

    async def str_to_chat_id(self, chat: str) -> int:
        return await self.session.str_to_chat_id(chat)

    async def format_dialog_html(self, chat_id: int):
        # TODO: handle PM URL
        name = await self.translate_chat_id(chat_id)
        return f'<a href = "https://t.me/c/{chat_id}/99999999">{html.escape(name)}</a> ({chat_id})'

    def _should_monitor(self, chat_id: int):
        # tell if a chat should be monitored
        share_id = get_share_id(chat_id)
        if self._cfg.monitor_all:
            return share_id not in self.excluded_chats
        else:
            return share_id in self.monitored_chats

    @staticmethod
    def _extract_text(event):
        if hasattr(event, 'raw_text') and event.raw_text and len(event.raw_text.strip()) >= 0:
            return escape_content(event.raw_text.strip())
        else:
            return ''

    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        # empty string will be returned if no sender
        sender = await message.get_sender()
        if isinstance(sender, User):
            return format_entity_name(sender)
        else:
            return ''

    def _register_hooks(self):
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            if self._should_monitor(event.chat_id):
                if msg_text := self._extract_text(event):  # pure text index insertion
                    share_id = get_share_id(event.chat_id)
                    sender = await self._get_sender_name(event.message)
                    url = f'https://t.me/c/{share_id}/{event.id}'
                    self._logger.info(f'New msg {url} from "{sender}": "{brief_content(msg_text)}"')
                    msg = IndexMsg(
                        content=msg_text,
                        url=url,
                        chat_id=share_id,
                        post_time=datetime.fromtimestamp(event.date.timestamp()),
                        sender=sender
                    )
                    self.newest_msg[share_id] = msg
                    self._indexer.add_document(msg)
                if self._cfg.cloudstorage:
                    await self.cloud_upload_message(event)

        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if self._should_monitor(event.chat_id) and (msg_text := self._extract_text(event)):
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                self._logger.info(f'Update message {url} to: "{brief_content(msg_text)}"')
                self._indexer.update(url=url, content=msg_text)
                if self._cfg.cloudstorage:
                    await self.cloud_upload_message(event, skip_existing=False)

        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None:
                return
            if self._should_monitor(event.chat_id):
                share_id = get_share_id(event.chat_id)
                for msg_id in event.deleted_ids:
                    url = f'https://t.me/c/{share_id}/{msg_id}'
                    self._logger.info(f'Delete message {url}')
                    self._indexer.delete(url=url)

    async def cloud_upload_message(self, event, skip_existing: bool = True):
        if not self._cloud_client:
            self._logger.warning('Cloud storage is not configured, skipping upload')
            return
        try:
            message = event.message if isinstance(event, events.NewMessage.Event) else event
            chat_id = get_share_id(event.chat_id)
            message_id = message.id
            if await self._cloud_client.check_message_exist(
                    chat_id=chat_id,
                    message_id=message_id
            ) and skip_existing:
                self._logger.info(f'Message {chat_id}-{message_id} already exists in cloud storage, skipping upload')
                return

            text = message.text
            timestamp = message.date.timestamp()
            sender = message.sender
            user_id = sender.id if sender else 0
            username = sender.username if sender else ''
            first_name = sender.first_name if sender else ''
            last_name = sender.last_name if sender else ''
            is_bot = sender.bot if sender else False

            telegram_user = TelegramUser(
                user_id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                is_bot=is_bot,
            )

            telegram_message = TelegramMessage(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                timestamp=timestamp,
                user=telegram_user,
            )

            if message.reply_to:
                telegram_message.reply_to = message.reply_to.reply_to_msg_id

            if message.fwd_from:
                if isinstance(message.fwd_from.from_id, PeerChannel):
                    fwd_from = message.fwd_from.from_id.channel_id
                else:
                    fwd_from = message.fwd_from.from_id.user_id
                telegram_message.fwd_from = fwd_from
                telegram_message.is_forward = True
            else:
                telegram_message.is_forward = False

            # upload media to cloud storage
            file_name = ""
            if message.media:
                media_type = 'unknown'
                if isinstance(message.media, MessageMediaPhoto):
                    media_type = 'photo'
                elif isinstance(message.media, MessageMediaDocument):
                    doc = message.media.document
                    if any(isinstance(attr, DocumentAttributeAudio) for attr in doc.attributes):
                        media_type = 'audio'
                    elif any(isinstance(attr, DocumentAttributeVideo) for attr in doc.attributes):
                        media_type = 'video'
                    else:
                        media_type = 'document'
                        file_name = message.file.name
                io_object = BytesIO()
                await message.download_media(file=io_object)
                io_object.seek(0)
                extension = message.file.ext if message.file else ''
                if file_name == "":
                    file_name = f'{chat_id}_{message_id}_{int(timestamp)}{extension}'
                media_key = await self._cloud_client.upload_to_s3(
                    file_obj=io_object,
                    s3_prefix=f'{chat_id}',
                    file_name=file_name
                )
                telegram_media = TelegramMedia(
                    media_key=media_key,
                    media_type=media_type
                )
                telegram_message.media = telegram_media
            # store message to cloud database
            await self._cloud_client.put_item_to_dynamo(
                item=telegram_message.dict(by_alias=True)
            )
        except Exception as e:
            self._logger.error(f'Failed to upload message {event.id} to cloud: {e}')
