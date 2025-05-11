import asyncio

import aioboto3

from io import BytesIO

from tg_searcher.common import get_logger

DEFAULT_TABLE_NAME = "tg_message_db"
DEFAULT_KEY_SCHEMA = [
    {"AttributeName": "chatId", "KeyType": "HASH"},
    {"AttributeName": "timestamp", "KeyType": "RANGE"},
]
DEFAULT_ATTRIBUTE_DEFINITIONS = [
    {"AttributeName": "chatId", "AttributeType": "N"},
    {"AttributeName": "timestamp", "AttributeType": "N"},
]


class AWSConfig:
    def __init__(self, **kw):
        self.region_name = kw.get('region_name', None)
        self.s3_bucket_name = kw.get('s3_bucket_name', None)
        self.dynamo_table_name = kw.get('dynamo_table_name', DEFAULT_TABLE_NAME)



class AWSClient:
    def __init__(self, cfg: AWSConfig):
        self._cfg: AWSConfig = cfg
        self._logger = get_logger(f'aws-client')
        self._session = aioboto3.Session(region_name=cfg.region_name)

    async def create_s3_bucket(self, bucket_name: str = None):
        if bucket_name is None:
            bucket_name = self._cfg.s3_bucket_name
        async with self._session.client('s3') as client:
            try:
                await client.create_bucket(Bucket=bucket_name)
                self._logger.info(f"Bucket {bucket_name} created successfully.")
                await client.put_public_access_block(
                    Bucket=bucket_name,
                    PublicAccessBlockConfiguration={
                        'BlockPublicAcls': True,
                        'IgnorePublicAcls': True,
                        'BlockPublicPolicy': True,
                        'RestrictPublicBuckets': True
                    }
                )
                self._logger.info(f"Public access blocked for bucket {bucket_name}.")
                await client.put_bucket_encryption(
                    Bucket=bucket_name,
                    ServerSideEncryptionConfiguration={
                        'Rules': [
                            {
                                'ApplyServerSideEncryptionByDefault': {
                                    'SSEAlgorithm': 'AES256'
                                }
                            }
                        ]
                    }
                )
                self._logger.info(f"Encryption enabled for bucket {bucket_name}.")
                await client.put_bucket_versioning(
                    Bucket=bucket_name,
                    VersioningConfiguration={
                        'Status': 'Enabled'
                    }
                )
            except client.exceptions.BucketAlreadyOwnedByYou as e:
                self._logger.warning(f"Bucket {bucket_name} already exists: {e}")

    async def create_dynamo_table(self, table_name: str = None, key_schema: list = None,
                                  attribute_definitions: list = None):
        if table_name is None:
            table_name = self._cfg.dynamo_table_name
        if attribute_definitions is None:
            attribute_definitions = DEFAULT_ATTRIBUTE_DEFINITIONS
        if key_schema is None:
            key_schema = DEFAULT_KEY_SCHEMA
        async with self._session.client('dynamodb') as client:
            try:
                await client.create_table(
                    TableName=table_name,
                    KeySchema=key_schema,
                    AttributeDefinitions=attribute_definitions,
                    BillingMode="PAY_PER_REQUEST",
                )
                while True:
                    resp = await client.describe_table(TableName=table_name)
                    if resp["Table"]["TableStatus"] == "ACTIVE":
                        print(f"Table {table_name} is ready.")
                        break
                    await asyncio.sleep(1)
                self._logger.info(f"Table {table_name} created successfully.")
            except client.exceptions.ResourceInUseException as e:
                self._logger.warning(f"Table {table_name} already exists: {e}")

    async def upload_to_s3(self, file_obj: BytesIO = None, file_path: str = None, s3_prefix: str = "undefined_chat",
                           file_name: str = "default_name") -> str:
        async with self._session.client('s3') as s3:
            try:
                if file_obj:
                    await s3.upload_fileobj(file_obj, self._cfg.s3_bucket_name, f"{s3_prefix}/{file_name}")
                elif file_path:
                    await s3.upload_file(file_path, self._cfg.s3_bucket_name, f"{s3_prefix}/{file_name}")
                return f"{self._cfg.s3_bucket_name}/{s3_prefix}/{file_name}"
            except Exception as e:
                raise RuntimeError(f"Failed to upload file to S3: {e}")

    async def put_item_to_dynamo(self, table_name: str = None, item: dict = None) -> None:
        if item is None:
            raise ValueError("Item cannot be None")
        if table_name is None:
            table_name = self._cfg.dynamo_table_name
        async with self._session.resource("dynamodb", region_name=self._cfg.region_name) as dynamodb:
            table = await dynamodb.Table(table_name)
            try:
                await table.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(chat_id) AND  attribute_not_exists(#ts)",
                    ExpressionAttributeNames={"#ts": "timestamp"}
                )
            except Exception as e:
                raise RuntimeError(f"Failed to put item to DynamoDB: {e}")

    async def generate_presigned_url(self, key: str, expires_in: int = 600) -> str:
        async with self._session.client("s3", region_name=self._cfg.region_name) as s3:
            try:
                return await s3.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={'Bucket': self._cfg.s3_bucket_name, 'Key': key},
                    ExpiresIn=expires_in
                )
            except Exception as e:
                raise RuntimeError(f"Failed to generate pre-signed URL: {e}")
