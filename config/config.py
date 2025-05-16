import os
import asyncio
from fake_useragent import UserAgent

# Initialize UserAgent in executor to avoid blocking
_ua = None

def _init_user_agent():
    try:
        return UserAgent(fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    except Exception:
        print("Lỗi khi khởi tạo UserAgent, sử dụng fallback cố định.")
        return None

# Schedule UA initialization in background
async def _get_user_agent():
    global _ua
    if _ua is None:
        loop = asyncio.get_event_loop()
        _ua = await loop.run_in_executor(None, _init_user_agent)
    return _ua

async def get_random_headers():
    """
    Bất đồng bộ: trả về headers với User-Agent ngẫu nhiên.
    """
    ua = await _get_user_agent()
    user_agent_string = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

    if ua:
        try:
            # Lấy User-Agent ngẫu nhiên
            user_agent_string = await asyncio.get_event_loop().run_in_executor(None, lambda: ua.random)
        except Exception as e:
            print(f"Lỗi khi lấy User-Agent ngẫu nhiên: {e}. Sử dụng fallback.")

    from config.config import BASE_URL  # avoid circular import issues
    headers = {
        "User-Agent": user_agent_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    }
    if BASE_URL:
        headers["Referer"] = BASE_URL.rstrip('/') + '/'
    return headers

# --- Cấu hình cơ bản ---
BASE_URL = "https://truyenfull.vision"
REQUEST_DELAY = 1  # Giây, độ trễ giữa các request
DATA_FOLDER = "truyen_data"
PROXIES_FOLDER = "proxies"
PROXIES_FILE = os.path.join(PROXIES_FOLDER, "proxies.txt")

# --- Thông tin đăng nhập Proxy (từ env) ---
GLOBAL_PROXY_USERNAME = os.getenv("PROXY_USER", "muonroi-zone-resi")
GLOBAL_PROXY_PASSWORD = os.getenv("PROXY_PASS", "0967442142")

# --- Mặc định nếu cần sync headers ---
def get_default_headers():
    from fake_useragent import UserAgent as _UA
    try:
        ua = _UA()
        ua_string = ua.random
    except Exception:
        ua_string = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    headers = {
        "User-Agent": ua_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    }
    if BASE_URL:
        headers["Referer"] = BASE_URL.rstrip('/') + '/'
    return headers

# --- LIMITS & OPTIONS ---
MAX_GENRES_TO_CRAWL = None
MAX_STORIES_PER_GENRE_PAGE = None
MAX_STORIES_TOTAL_PER_GENRE = None
MAX_CHAPTERS_PER_STORY = None
MAX_CHAPTER_PAGES_TO_CRAWL = None
RETRY_FAILED_CHAPTERS_PASSES = 4

TIMEOUT_REQUEST = 30
RETRY_ATTEMPTS = 3
DELAY_ON_RETRY = 5

STATE_FILE = "crawl_state.json"
