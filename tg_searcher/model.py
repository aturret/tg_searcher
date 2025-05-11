from pydantic import BaseModel, Field

from enum import Enum
from typing import Optional
from datetime import datetime


class MediaType(str, Enum):
    PHOTO = "photo"
    VIDEO = "video"
    AUDIO = "audio"
    DOCUMENT = "document"
    UNKNOWN = "unknown"


class BaseSchema(BaseModel):
    class Config:
        use_enum_values = True
        populate_by_name = True


class TelegramUser(BaseSchema):
    user_id: int = Field(..., alias='userId')
    username: Optional[str] = Field(None, alias='username')
    first_name: Optional[str] = Field(None, alias='firstName')
    last_name: Optional[str] = Field(None, alias='lastName')
    is_bot: bool = Field(False, alias='isBot')


class TelegramMedia(BaseSchema):
    media_type: MediaType = Field(..., alias='mediaType')
    media_key: str = Field(..., alias='mediaKey')


class TelegramMessage(BaseSchema):
    chat_id: int = Field(..., alias='chatId')
    message_id: int = Field(..., alias='messageId')
    timestamp: int = Field(..., alias='timestamp')
    user: TelegramUser = Field(..., alias='user')
    text: Optional[str] = Field(None, alias='text')
    media: Optional[TelegramMedia] = Field(None, alias='media')
    reply_to: Optional[int] = Field(None, alias='replyTo')
    is_forward: Optional[bool] = Field(None, alias='isForward')
    fwd_from: Optional[int] = Field(None, alias='fwdFrom')
