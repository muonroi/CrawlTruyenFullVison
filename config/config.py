import os
import asyncio
import re
from dotenv import load_dotenv
from config.useragent_list import STATIC_USER_AGENTS

# Load env từ .env hoặc hệ thống
load_dotenv()

# ============ BASE URLs ============
BASE_URLS = {
    "truyenfull": os.getenv("BASE_TRUYENFULL", "https://truyenfull.vision"),
    "metruyenfull": os.getenv("BASE_METRUYENFULL", "https://metruyenfull.net"),
    "truyenyy": os.getenv("BASE_TRUYENYY", "https://truyenyy.co"),
}

# ============ CRAWL CONFIG ============
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "5"))
TIMEOUT_REQUEST = int(os.getenv("TIMEOUT_REQUEST", 30))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", 3))
DELAY_ON_RETRY = float(os.getenv("DELAY_ON_RETRY", 2.5))

# ============ FOLDER CONFIG ============
DATA_FOLDER = os.getenv("DATA_FOLDER", "truyen_data")
COMPLETED_FOLDER = os.getenv("COMPLETED_FOLDER", "completed_stories")
BACKUP_FOLDER = os.getenv("BACKUP_FOLDER", "backup_truyen_data")
STATE_FOLDER = os.getenv("STATE_FOLDER", "state")
LOG_FOLDER = os.getenv("LOG_FOLDER", "logs")

os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(COMPLETED_FOLDER, exist_ok=True)
os.makedirs(BACKUP_FOLDER, exist_ok=True)
os.makedirs(STATE_FOLDER, exist_ok=True)
os.makedirs(LOG_FOLDER, exist_ok=True)

# ============ RETRY / BATCH CONFIG ============
RETRY_GENRE_ROUND_LIMIT = int(os.getenv("RETRY_GENRE_ROUND_LIMIT", 3))
RETRY_SLEEP_SECONDS = int(os.getenv("RETRY_SLEEP_SECONDS", 30 * 60))
RETRY_FAILED_CHAPTERS_PASSES = int(os.getenv("RETRY_FAILED_CHAPTERS_PASSES", 2))
NUM_CHAPTER_BATCHES = int(os.getenv("NUM_CHAPTER_BATCHES", 10))
MAX_CHAPTERS_PER_STORY = int(os.getenv("MAX_CHAPTERS_PER_STORY", 0)) or None
RETRY_STORY_ROUND_LIMIT = int(os.getenv("RETRY_STORY_ROUND_LIMIT", 40))

# ============ PROXY CONFIG ============
USE_PROXY = os.getenv("USE_PROXY", "false").lower() == "true"
PROXIES_FOLDER = os.getenv("PROXIES_FOLDER", "proxies")
PROXIES_FILE = os.getenv("PROXIES_FILE", os.path.join(PROXIES_FOLDER, "proxies.txt"))
GLOBAL_PROXY_USERNAME = os.getenv("PROXY_USER")
GLOBAL_PROXY_PASSWORD = os.getenv("PROXY_PASS")
LOADED_PROXIES = []
LOCK = asyncio.Lock()

# ============ TELEGRAM ============
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ============ LIMIT CRAWL ============
MAX_GENRES_TO_CRAWL = int(os.getenv("MAX_GENRES_TO_CRAWL", 0)) or None
MAX_STORIES_PER_GENRE_PAGE = int(os.getenv("MAX_STORIES_PER_GENRE_PAGE", 0)) or None
MAX_STORIES_TOTAL_PER_GENRE = int(os.getenv("MAX_STORIES_TOTAL_PER_GENRE", 0)) or None
MAX_CHAPTER_PAGES_TO_CRAWL = int(os.getenv("MAX_CHAPTER_PAGES_TO_CRAWL", 0)) or None

# ============ ASYNC LIMIT ============
ASYNC_SEMAPHORE_LIMIT = int(os.getenv("ASYNC_SEMAPHORE_LIMIT", 5))
GENRE_ASYNC_LIMIT = int(os.getenv("GENRE_ASYNC_LIMIT", 3))
GENRE_BATCH_SIZE = int(os.getenv("GENRE_BATCH_SIZE", 3))
STORY_ASYNC_LIMIT = int(os.getenv("STORY_ASYNC_LIMIT", 3))
STORY_BATCH_SIZE = int(os.getenv("STORY_BATCH_SIZE", 3))

# ============ FILE PATH ============
FAILED_GENRES_FILE = os.getenv("FAILED_GENRES_FILE", "failed_genres.json")
PATTERN_FILE = os.getenv("PATTERN_FILE", "config/blacklist_patterns.txt")
ANTI_BOT_PATTERN_FILE = os.getenv("ANTI_BOT_PATTERN_FILE", "config/anti_bot_patterns.txt")

# ============ PARSING RULE ============
def truyenyy_selector(soup):
    # Bảo thủ: tìm article có cả "flex" và "flex-col"
    for tag in soup.find_all("article"):
        classes = tag.get("class", [])
        if "flex" in classes and "flex-col" in classes:
            return tag
    return None
SITE_SELECTORS = {
    "truyenfull": lambda soup: soup.find("div", id=re.compile(r'\bchapter-c\b', re.I)),
    "truyenyy": truyenyy_selector,
    "metruyenfull": lambda soup: soup.select_one("div.chapter-content"),
}
HEADER_PATTERNS = [r"^nguồn:", r"^truyện:", r"^thể loại:", r"^chương:"]
HEADER_RE = re.compile("|".join(HEADER_PATTERNS), re.IGNORECASE)

# ============ User-Agent ============
_UA_OBJ = None
_DISABLE_FAKE_UA = False

def _init_user_agent():
    try:
        from fake_useragent import UserAgent
        return UserAgent(fallback=STATIC_USER_AGENTS[0])
    except Exception as e:
        print(f"Lỗi khi khởi tạo UserAgent: {e}")
        return None

import random
async def get_random_user_agent():
    global _UA_OBJ, _DISABLE_FAKE_UA
    if _DISABLE_FAKE_UA:
        return random.choice(STATIC_USER_AGENTS)
    if _UA_OBJ is None:
        loop = asyncio.get_event_loop()
        _UA_OBJ = await loop.run_in_executor(None, _init_user_agent)
        if _UA_OBJ is None:
            _DISABLE_FAKE_UA = True
            return random.choice(STATIC_USER_AGENTS)
    try:
        return await asyncio.get_event_loop().run_in_executor(None, lambda: _UA_OBJ.random) #type: ignore
    except Exception:
        _DISABLE_FAKE_UA = True
        return random.choice(STATIC_USER_AGENTS)

async def get_random_headers(site_key):
    ua_string = await get_random_user_agent()
    headers = {
        "User-Agent": ua_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
    }
    base_url = BASE_URLS.get(site_key)
    if base_url:
        headers["Referer"] = base_url.rstrip('/') + '/'
    return headers

def get_state_file(site_key: str) -> str:
    return os.path.join(STATE_FOLDER, f"crawl_state_{site_key}.json")

def load_blacklist_patterns(file_path):
    patterns, contains_list = [], []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith('^') or line.endswith('$') or re.search(r'[.*?|\[\]()\\]', line):
                patterns.append(re.compile(line, re.IGNORECASE))
            else:
                contains_list.append(line.lower())
    return patterns, contains_list
