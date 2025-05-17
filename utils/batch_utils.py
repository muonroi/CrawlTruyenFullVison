import asyncio
import random
from config.config import REQUEST_DELAY


def split_batches(items, num_batches):
    batch_size = max(1, len(items) // num_batches + (len(items) % num_batches > 0))
    return [items[i:i+batch_size] for i in range(0, len(items), batch_size)]



async def smart_delay(base=REQUEST_DELAY):
    delay = random.uniform(base*0.7, base*1.3)
    await asyncio.sleep(delay)