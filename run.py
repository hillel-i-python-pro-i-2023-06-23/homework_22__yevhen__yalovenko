import asyncio

from crawler.concurrency.async_client import main
from crawler.logging.init_logging import init_logging

if __name__ == "__main__":
    init_logging()

    asyncio.run(main())
