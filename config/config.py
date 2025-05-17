# config.py
import os
import asyncio
import re
from dotenv import load_dotenv

# Load env (ưu tiên .env > biến hệ thống)
load_dotenv()

# -------------- [Cấu hình CƠ BẢN] --------------
BASE_URL = os.getenv("BASE_URL", "https://truyenfull.vision")
BASE_METRUYENFULL_URL = "https://metruyenfull.net"
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "5"))  # Giây delay giữa các request
DATA_FOLDER = os.getenv("DATA_FOLDER", "truyen_data")
COMPLETED_FOLDER = os.getenv("COMPLETED_FOLDER", "completed_stories")
BACKUP_FOLDER = os.getenv("BACKUP_FOLDER", "backup_truyen_data")

# -------------- [Proxy & User-Agent] --------------
USE_PROXY = os.getenv("USE_PROXY", "True") == "True"
PROXIES_FOLDER = os.getenv("PROXIES_FOLDER", "proxies")
PROXIES_FILE = os.getenv("PROXIES_FILE", os.path.join(PROXIES_FOLDER, "proxies.txt"))
GLOBAL_PROXY_USERNAME = os.getenv("PROXY_USER")
GLOBAL_PROXY_PASSWORD = os.getenv("PROXY_PASS")
LOADED_PROXIES = []  # global list (sẽ được load khi chạy)
LOCK = asyncio.Lock()

# -------------- [Telegram Notify] --------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# -------------- [Async và batch crawling] --------------
ASYNC_SEMAPHORE_LIMIT = int(os.getenv("ASYNC_SEMAPHORE_LIMIT", 5))
GENRE_ASYNC_LIMIT = int(os.getenv("GENRE_ASYNC_LIMIT", 3))
GENRE_BATCH_SIZE = int(os.getenv("GENRE_BATCH_SIZE", 3))
NUM_CHAPTER_BATCHES = int(os.getenv("NUM_CHAPTER_BATCHES", 10))

# -------------- [Limit crawl] --------------
MAX_GENRES_TO_CRAWL = int(os.getenv("MAX_GENRES_TO_CRAWL", 0)) or None
MAX_STORIES_PER_GENRE_PAGE = int(os.getenv("MAX_STORIES_PER_GENRE_PAGE", 0)) or None
MAX_STORIES_TOTAL_PER_GENRE = int(os.getenv("MAX_STORIES_TOTAL_PER_GENRE", 0)) or None
MAX_CHAPTERS_PER_STORY = int(os.getenv("MAX_CHAPTERS_PER_STORY", 0)) or None
MAX_CHAPTER_PAGES_TO_CRAWL = int(os.getenv("MAX_CHAPTER_PAGES_TO_CRAWL", 0)) or None
RETRY_FAILED_CHAPTERS_PASSES = int(os.getenv("RETRY_FAILED_CHAPTERS_PASSES", 2))
TIMEOUT_REQUEST = int(os.getenv("TIMEOUT_REQUEST", 30))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", 3))
DELAY_ON_RETRY = float(os.getenv("DELAY_ON_RETRY", 2.5))
# -------------- [Files & State] --------------
STATE_FILE = os.getenv("STATE_FILE", "crawl_state.json")
ERROR_CHAPTERS_FILE = os.getenv("ERROR_CHAPTERS_FILE", "error_chapters.json")
PATTERN_FILE = os.getenv("PATTERN_FILE", "config/blacklist_patterns.txt")

# -------------- [Regex patterns] --------------
HEADER_PATTERNS = [
    r"^nguồn:",
    r"^truyện:",
    r"^thể loại:",
    r"^chương:",
]
HEADER_RE = re.compile("|".join(HEADER_PATTERNS), re.IGNORECASE)

# -------------- [User-Agent] --------------
# Import list từ file riêng hoặc khai báo ở đây
from config.useragent_list import STATIC_USER_AGENTS  # bạn tạo file này bên dưới
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
        ua_str = await asyncio.get_event_loop().run_in_executor(None, lambda: _UA_OBJ.random) # type: ignore
        if ua_str:
            return ua_str
        raise Exception("fake_useragent trả về UA rỗng")
    except Exception as e:
        print(f"Lỗi lấy UA từ fake_useragent: {e}. Sử dụng random static UA.")
        _DISABLE_FAKE_UA = True
        return random.choice(STATIC_USER_AGENTS)

async def get_random_headers():
    ua_string = await get_random_user_agent()
    headers = {
        "User-Agent": ua_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    }
    if BASE_URL:
        headers["Referer"] = BASE_URL.rstrip('/') + '/'
    return headers

def load_blacklist_patterns(file_path):
    patterns = []
    contains_list = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Regex pattern nếu có ^, $, hoặc kí tự regex đặc biệt
            if line.startswith('^') or line.endswith('$') or re.search(r'[.*?|\[\]()\\]', line):
                patterns.append(re.compile(line, re.IGNORECASE))
            else:
                contains_list.append(line.lower())
    return patterns, contains_list
