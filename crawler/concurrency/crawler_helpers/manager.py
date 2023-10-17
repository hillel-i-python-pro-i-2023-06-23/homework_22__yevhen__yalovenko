import asyncio
from typing import Any
from collections.abc import Iterable

from example_2.concurrency.example_2_helpers.worker import worker


async def manager(data: Iterable[Any], workers_amount: int = 5) -> list[str]:
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    for item in data:
        await input_queue.put(item)

    workers = [
        worker(
            input_queue=input_queue,
            output_queue=output_queue,
            worker_number=number,
        )
        for number in range(0, workers_amount)
    ]

    await asyncio.gather(
        *workers,
    )

    results = []
    while not output_queue.empty():
        result = await output_queue.get()
        results.append(result)

    return results
