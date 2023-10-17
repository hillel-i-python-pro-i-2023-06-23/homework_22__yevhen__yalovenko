from faker import Faker

from example_2.concurrency.example_2_helpers.manager import manager
from example_2.concurrency.example_2_helpers.some_class import SomeClass


async def main():
    faker = Faker()

    # items = range(0, 100)
    items = [SomeClass(name=faker.name()) for _ in range(0, 50)]

    results = await manager(data=items, workers_amount=5)

    print(results)
