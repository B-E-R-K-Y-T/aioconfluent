import asyncio
import os
import uuid
from asyncio import iscoroutinefunction
from typing import Callable

_semaphore_thread = asyncio.Semaphore(os.cpu_count())


async def run_async_thread(func: Callable, args: tuple, **kwargs):
    if iscoroutinefunction(func):
        return await func(*args, **kwargs)

    async with _semaphore_thread:
        return await asyncio.to_thread(func, *args, **kwargs)


async def run_async(func: Callable, args: tuple, **kwargs):
    if iscoroutinefunction(func):
        return await func(*args, **kwargs)

    return func(*args, **kwargs)


def gen_unique_id():
    return str(uuid.uuid4())


class SerialNumberGenerator:
    counter = 0

    def __new__(cls):
        cls.counter += 1
        return super().__new__(cls)
