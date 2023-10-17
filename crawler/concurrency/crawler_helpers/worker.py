import asyncio

from example_2.concurrency.example_2_helpers.some_task import some_task
from example_2.logging.loggers import get_custom_logger


async def worker(
    input_queue: asyncio.Queue,
    output_queue: asyncio.Queue,
    worker_number: int,
    #
    timeout: float = 5,
) -> None:
    worker_name = f"[worker-{worker_number}]"
    logger = get_custom_logger(name=worker_name)

    while True:
        try:
            item = await asyncio.wait_for(input_queue.get(), timeout=timeout)
        except TimeoutError:
            logger.info("timeout")
            return

        logger.info(f"{item} start")
        result = await some_task(item=item, logger=logger)
        logger.info(f"{item} end")

        await output_queue.put(result)
