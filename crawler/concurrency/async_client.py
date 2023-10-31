import asyncio
import logging
from collections import deque
from typing import TypeAlias
from urllib.parse import urlparse, urljoin

import aiohttp
import aiosqlite
import bs4

from crawler.config import DB_PATH, URLS, LOGS_PATH
from crawler.logging.loggers import get_custom_logger
from crawler.services.create_table import create_table
from crawler.services.get_urls_to_print import get_all_urls

T_URL: TypeAlias = str
T_URLS: TypeAlias = list[T_URL]
T_URLS_AS_SET: TypeAlias = set[T_URL]
T_TEXT: TypeAlias = str

BATCH_SIZE = 10

semaphore = asyncio.Semaphore(10)


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
        async with semaphore:
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


async def handle_url(url: T_URL, session: aiohttp.ClientSession, depth: int, queue: deque) -> T_URLS:
    logger = get_custom_logger(name=url)

    if url is None:
        logger.info(f"Invalid URL: {url}")
        return []

    if not url.startswith("http://") and not url.startswith("https://"):
        logger.info(f"Invalid URL: {url}")
        return []

    async with aiosqlite.connect(DB_PATH) as connection:
        cursor = await connection.execute("SELECT * FROM urls WHERE url = ?", (url,))
        result = await cursor.fetchone()

        if result is not None:
            logger.info(f"URL {url} is already in the database")
            return []

        if depth == 0:
            logger.info(f"Reached the maximum depth for URL {url}")
            return []

        await connection.execute("INSERT OR IGNORE INTO urls (url) VALUES (?)", (url,))
        await connection.commit()

    text = await make_request(url=url, session=session, logger=logger)

    urls_as_set = await get_urls_from_text(text=text)

    new_urls = list(urls_as_set)

    base_url = get_base_url(url)

    for new_url in new_urls:
        new_url = normalize_url(new_url, base_url)
        if new_url and same_domain(new_url, base_url):
            queue.append((new_url, depth - 1))

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
    urls = URLS
    await create_table()

    depth = 2

    queue = deque()

    for url in urls:
        queue.append((url, depth))

    async with aiohttp.ClientSession() as session:
        while queue:
            tasks = []
            for _ in range(BATCH_SIZE):
                if queue:
                    url, depth = queue.popleft()
                    tasks.append(handle_url(url=url, session=session, depth=depth, queue=queue))
            await asyncio.gather(*tasks)

    urls = await get_all_urls()
    print("The following sites are in the database:")
    for url in urls:
        print(url)
    print(f"Also you can check logs {LOGS_PATH.as_uri()}")
