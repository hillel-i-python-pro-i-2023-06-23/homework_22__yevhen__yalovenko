import aiosqlite

from crawler.config import DB_PATH


async def create_table():
    async with aiosqlite.connect(DB_PATH) as connection:
        async with connection.execute(
            """
                CREATE TABLE IF NOT EXISTS urls (url TEXT PRIMARY KEY)
            """
        ):
            ...
