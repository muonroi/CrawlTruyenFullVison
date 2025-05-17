import asyncio
import random
from config.config import REQUEST_DELAY


def split_batches(lst, num_batches):
    k, m = divmod(len(lst), num_batches)
    batches = []
    start = 0
    for i in range(num_batches):
        end = start + k + (1 if i < m else 0)
        batches.append(lst[start:end])
        start = end
    return batches

async def smart_delay(base=REQUEST_DELAY):
    delay = random.uniform(base*0.7, base*1.3)
    await asyncio.sleep(delay)