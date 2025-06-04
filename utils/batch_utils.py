import asyncio
import random
from config.config import REQUEST_DELAY


def split_batches(items, num_batches):
    avg = len(items) // num_batches
    remainder = len(items) % num_batches
    batches = []
    start = 0
    for i in range(num_batches):
        batch_size = avg + (1 if i < remainder else 0)
        batches.append(items[start:start+batch_size])
        start += batch_size
    return [b for b in batches if b]



async def smart_delay(base=REQUEST_DELAY):
    delay = random.uniform(base*0.7, base*1.3)
    await asyncio.sleep(delay)


def get_optimal_batch_size(num_missing, context="chapter"):
    if context == "genre":
        if num_missing <= 5: return 1
        elif num_missing <= 20: return 2
        else: return 5
    if num_missing <= 5: return 1
    elif num_missing <= 20: return 2
    elif num_missing <= 100: return 10
    else: return 30

