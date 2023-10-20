import asyncio
import logging
from pprint import pprint
from typing import TypeAlias

import aiohttp
import bs4

from crawler.logging.loggers import get_custom_logger
from crawler.services.create_table import create_table
from crawler.services.db_connection import DBConnection

# # For url parsing.
# import urllib.parse


T_URL: TypeAlias = str
T_URLS: TypeAlias = list[T_URL]
T_URLS_AS_SET: TypeAlias = set[T_URL]

T_TEXT: TypeAlias = str


async def get_urls_from_text(text: T_TEXT) -> T_URLS_AS_SET:
    soup = bs4.BeautifulSoup(markup=text, features="html.parser")

    urls = set()
    for link_element in soup.find_all("a"):
        url = link_element.get("href")
        urls.add(url)

    return set(urls)


async def make_request(
    url: T_URL,
    session: aiohttp.ClientSession,
    logger: logging.Logger,
) -> T_TEXT:
    async with session.get(url) as response:
        logger.info(response.status)
        return await response.text()


async def handle_url(url: T_URL, session: aiohttp.ClientSession, depth: int) -> T_URLS:
    logger = get_custom_logger(name=url)

    if url is None:
        logger.info(f"Invalid URL: {url}")
        return []

    if not url.startswith("http://") and not url.startswith("https://"):
        logger.info(f"Invalid URL: {url}")
        return []

    with DBConnection() as connection:
        result = connection.execute("SELECT * FROM urls WHERE url = ?", (url,)).fetchone()

        if result is not None:
            logger.info(f"URL {url} is already in the database")
            return []

        if depth == 0:
            logger.info(f"Reached the maximum depth for URL {url}")
            return []

        connection.execute("INSERT INTO urls (url) VALUES (?)", (url,))
        connection.commit()

    text = await make_request(url=url, session=session, logger=logger)

    urls_as_set = await get_urls_from_text(text=text)

    new_urls = list(urls_as_set)

    for new_url in new_urls:
        await handle_url(new_url, session, depth - 1)

    return new_urls


async def main():
    urls = [
        "https://example.com/",
        "https://www.djangoproject.com/",
    ]
    create_table()

    depth = 2

    async with aiohttp.ClientSession() as session:
        tasks = [handle_url(url=url, session=session, depth=depth) for url in urls]

        results = await asyncio.gather(*tasks)

    for result in results:
        pprint(result)
