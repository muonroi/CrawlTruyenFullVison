import asyncio

from config.config import ASYNC_SEMAPHORE_LIMIT


SEM = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)