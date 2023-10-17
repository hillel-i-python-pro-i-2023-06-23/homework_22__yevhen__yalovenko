import asyncio
import logging
import random


async def some_task(item: int, logger: logging.Logger) -> str:
    time_to_wait = random.randint(0, 5)
    logger.info(f"'{item}' wait for {time_to_wait} seconds")
    await asyncio.sleep(time_to_wait)

    return f"{item} - done"
