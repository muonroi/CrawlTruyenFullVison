# config.py
import os

# --- Cấu hình cơ bản ---
BASE_URL = "https://truyenfull.vision"
REQUEST_DELAY = 1  # Giây, độ trễ giữa các request để tránh làm quá tải server
DATA_FOLDER = "truyen_data"  # Thư mục để lưu trữ truyện đã crawl
PROXIES_FOLDER = "proxies"
PROXIES_FILE = os.path.join(PROXIES_FOLDER, "proxies.txt")

# --- Thông tin đăng nhập Proxy (NÊN dùng biến môi trường hoặc file .env) ---
# Ví dụ: GLOBAL_PROXY_USERNAME = os.getenv("PROXY_USERNAME", "your_default_username_if_any")
GLOBAL_PROXY_USERNAME = "muonroi-zone-resi"
GLOBAL_PROXY_PASSWORD = "0967442142"

# --- Headers mặc định cho Scraper ---
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "Referer": BASE_URL + "/", # Sẽ được cập nhật nếu BASE_URL thay đổi
}

# --- Giới hạn Crawl ---
MAX_GENRES_TO_CRAWL = 2
MAX_STORIES_PER_GENRE_PAGE = 2
MAX_STORIES_TOTAL_PER_GENRE = 3
MAX_CHAPTERS_PER_STORY = 5
MAX_CHAPTER_PAGES_TO_CRAWL = 50 # Giới hạn số trang danh sách chương trên mỗi truyện
RETRY_FAILED_CHAPTERS_PASSES = 1

# --- Tùy chọn khác ---
TIMEOUT_REQUEST = 30 # Thời gian timeout cho mỗi request (giây)
RETRY_ATTEMPTS = 3 # Số lần thử lại request nếu thất bại
DELAY_ON_RETRY = 5 # Thời gian chờ giữa các lần thử lại (giây)