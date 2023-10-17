import asyncio
import logging
from typing import TypeAlias

import aiohttp
import bs4

from crawler.logging.init_logging import init_logging
from crawler.logging.loggers import get_custom_logger

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


async def handle_url(url: T_URL, session: aiohttp.ClientSession) -> T_URLS:
    logger = get_custom_logger(name=url)

    text = await make_request(url=url, session=session, logger=logger)

    urls_as_set = await get_urls_from_text(text=text)

    return list(urls_as_set)


async def main():
    urls = [
        "https://example.com/",
        "https://www.djangoproject.com/",
    ]

    async with aiohttp.ClientSession(
        # If you need to ignore cookies.
        # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.DummyCookieJar
        # cookie_jar=aiohttp.DummyCookieJar(),
    ) as session:
        tasks = [handle_url(url=url, session=session) for url in urls]

        results = await asyncio.gather(*tasks)

    for result in results:
        print(result)

    ...


if __name__ == "__main__":
    init_logging()

    asyncio.run(main())