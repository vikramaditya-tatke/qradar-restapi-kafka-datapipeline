from motor.motor_asyncio import AsyncIOMotorClient
from settings import settings
from urllib.parse import quote_plus
from pipeline_logger import logger



class MongoConnector:
    def __init__(self) -> None:
        try:
            mongodb_user = quote_plus(settings.mongodb_user)
            mongodb_pwd = quote_plus(str(settings.mongodb_password))
            self.client = AsyncIOMotorClient(
                f"mongodb://{mongodb_user}:{mongodb_pwd}@{settings.mongodb_host0}:{settings.mongodb_port},{settings.mongodb_host1}:{settings.mongodb_port},{settings.mongodb_host2}:{settings.mongodb_port},{settings.mongodb_host3}:{settings.mongodb_port}/?authMechanism=DEFAULT&authSource=admin"
            )
        except Exception as e:
            print(e)

    async def __aenter__(self):  # Asynchronous context manager
        return self

    @staticmethod
    async def make_collection(collection_name, database):
        time_field = "stageTimestamp_ms"
        expire_after_seconds = 2419200  # 28 days
        # expire_after_seconds = 15552000  # 180 days

        await database.create_collection(
            collection_name,
            timeseries={
                "timeField": time_field,
                "metaField": "WeekFrom",
                "granularity": "hours",
            },
            expireAfterSeconds=expire_after_seconds,
            check_exists=True,
        )

    # TODO: Replace the Exception with a narrower clause.
    async def insert_documents(self, database_name, collection_name, documents) -> int:
        """Asynchronously inserts multiple documents into the specified collection."""
        try:
            db = self.client.get_database(database_name)
            if collection_name not in await db.list_collection_names():
                await self.make_collection(collection_name, db)
            collection = db.get_collection(collection_name)
            result = await collection.insert_many(documents)
            return len(result.inserted_ids)
        except Exception as e:
            logger.exception("Couldn't insert documents")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
