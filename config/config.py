# config.py
import os

from fake_useragent import UserAgent

try:
    ua = UserAgent(fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
except Exception: # Bắt lỗi rộng hơn nếu UserAgent() không khởi tạo được
    ua = None # Hoặc đặt một giá trị mặc định cố định khác
    print("Lỗi khi khởi tạo UserAgent, sẽ sử dụng fallback User-Agent cố định.")


def get_random_headers():
    """
    Tạo một dictionary chứa headers với User-Agent ngẫu nhiên.
    """
    user_agent_string = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' # Fallback
    if ua:
        try:
            # Lấy một User-Agent ngẫu nhiên cho các trình duyệt phổ biến
            user_agent_string = ua.random
        except Exception as e:
            print(f"Lỗi khi lấy User-Agent ngẫu nhiên: {e}. Sử dụng fallback.")
            # user_agent_string vẫn là giá trị fallback đã đặt

    headers = {
        "User-Agent": user_agent_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
        # "Referer" sẽ được đặt linh động dựa trên BASE_URL hoặc URL trước đó
        # trong hàm initialize_scraper hoặc make_request
    }
    if BASE_URL: # Thêm Referer nếu BASE_URL được định nghĩa
        headers["Referer"] = BASE_URL + "/"
    return headers

# --- Cấu hình cơ bản ---
BASE_URL = "https://truyenfull.vision"
REQUEST_DELAY = 1  # Giây, độ trễ giữa các request để tránh làm quá tải server
DATA_FOLDER = "truyen_data"  # Thư mục để lưu trữ truyện đã crawl
PROXIES_FOLDER = "proxies"
PROXIES_FILE = os.path.join(PROXIES_FOLDER, "proxies.txt")

# --- Thông tin đăng nhập Proxy (NÊN dùng biến môi trường hoặc file .env) ---
GLOBAL_PROXY_USERNAME = "muonroi-zone-resi" # Thay bằng os.getenv("PROXY_USER")
GLOBAL_PROXY_PASSWORD = "0967442142"    # Thay bằng os.getenv("PROXY_PASS")

# --- Headers mặc định cho Scraper ---
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    # "Referer" sẽ được đặt trong hàm initialize_scraper dựa trên BASE_URL
}

# --- Giới hạn Crawl (Đặt None để không giới hạn) ---
MAX_GENRES_TO_CRAWL = None # Crawl tất cả thể loại
MAX_STORIES_PER_GENRE_PAGE = None # Crawl tất cả các trang danh sách truyện của một thể loại
MAX_STORIES_TOTAL_PER_GENRE = None # Crawl tất cả truyện trong một thể loại
MAX_CHAPTERS_PER_STORY = None # Crawl tất cả các chương của một truyện
MAX_CHAPTER_PAGES_TO_CRAWL = None # Crawl tất cả các trang danh sách chương của một truyện
RETRY_FAILED_CHAPTERS_PASSES = 2 # Tăng số lần thử lại nếu muốn (tùy chọn)

# --- Tùy chọn khác ---
TIMEOUT_REQUEST = 30 # Thời gian timeout cho mỗi request (giây)
RETRY_ATTEMPTS = 3 # Số lần thử lại request nếu thất bại (cho make_request)
DELAY_ON_RETRY = 5 # Thời gian chờ giữa các lần thử lại request (cho make_request)

# --- Cấu hình lưu vết (Resumability) ---
STATE_FILE = "crawl_state.json" # File để lưu trạng thái crawl