import aiosqlite

from crawler.config import DB_PATH


async def get_all_urls():
    """Get all urls in database"""
    urls = []

    async with aiosqlite.connect(DB_PATH) as connection:
        async with connection.execute("SELECT url FROM urls") as cursor:
            rows = await cursor.fetchall()

            for row in rows:
                urls.append(row[0])

    return urls
