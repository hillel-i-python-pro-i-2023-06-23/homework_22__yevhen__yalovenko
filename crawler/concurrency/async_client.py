import asyncio
import logging
from typing import TypeAlias
from urllib.parse import urljoin, urlparse

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
    try:
        async with session.get(url) as response:
            logger.info(response.status)
            if response.status == 200:
                return await response.text()
            else:
                logger.error(f"Failed to get {url}: {response.status}")
                return ""
    except Exception as e:
        logger.exception(f"Exception occurred while getting {url}: {e}")
        return ""


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

    base_url = get_base_url(url)

    for new_url in new_urls:
        new_url = normalize_url(new_url, base_url)
        if new_url and same_domain(new_url, base_url):
            await handle_url(new_url, session, depth - 1)

    return new_urls


def get_base_url(url: T_URL) -> T_URL:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def normalize_url(url: T_URL, base_url: T_URL) -> T_URL:
    if url is None:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    else:
        return urljoin(base_url, url)


def same_domain(url: T_URL, base_url: T_URL) -> bool:
    return get_base_url(url) == base_url


async def main():
    urls = [
        "https://example.com/",
        "https://www.djangoproject.com/",
    ]
    create_table()

    depth = 2

    async with aiohttp.ClientSession() as session:
        tasks = [handle_url(url=url, session=session, depth=depth) for url in urls]

        await asyncio.gather(*tasks)
