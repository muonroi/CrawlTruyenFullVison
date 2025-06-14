import re
from functools import lru_cache
from utils.io_utils import load_patterns
from config.config import ANTI_BOT_PATTERN_FILE

@lru_cache()
def get_anti_bot_patterns():
    return load_patterns(ANTI_BOT_PATTERN_FILE)

def is_anti_bot_content(text: str) -> bool:
    patterns = get_anti_bot_patterns()
    lower = text.lower()
    return any(p.search(lower) for p in patterns)

