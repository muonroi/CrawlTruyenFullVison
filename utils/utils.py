# utils.py
import os
import re
import json
import logging
from logging.handlers import RotatingFileHandler
from config.config import STATE_FILE
# --- Thiết lập Logging ---
LOG_FILE_PATH = "crawler.log"
# Tạo logger
logger = logging.getLogger("TruyenFullCrawler")
logger.setLevel(logging.DEBUG) # Ghi lại tất cả các level từ DEBUG trở lên

# Tạo console handler và đặt level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO) # Chỉ hiển thị INFO trở lên trên console

# Tạo file handler và đặt level
# RotatingFileHandler sẽ tạo file mới khi file log đạt kích thước nhất định
fh = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
fh.setLevel(logging.DEBUG) # Ghi DEBUG trở lên vào file

# Tạo formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Thêm formatter vào handlers
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# Thêm handlers vào logger
logger.addHandler(ch)
logger.addHandler(fh)

def load_crawl_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
                logger.info(f"Đã tải trạng thái crawl từ {STATE_FILE}: {state}")
                return state
        except Exception as e:
            logger.error(f"Lỗi khi tải trạng thái crawl từ {STATE_FILE}: {e}. Bắt đầu crawl mới.")
    return {}

def clear_specific_state_keys(state: dict, keys_to_remove: list):
    """
    Xóa các key cụ thể khỏi dictionary trạng thái và lưu lại trạng thái.
    Hàm này thay đổi trực tiếp dict 'state' được truyền vào.
    """
    updated = False
    for key in keys_to_remove:
        if key in state:
            del state[key]
            updated = True
            logger.debug(f"Đã xóa key '{key}' khỏi trạng thái crawl.")
    if updated:
        save_crawl_state(state) # Lưu lại state sau khi đã xóa các key

def save_crawl_state(state: dict):
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=4)
        logger.info(f"Đã lưu trạng thái crawl vào {STATE_FILE}")
    except Exception as e:
        logger.error(f"Lỗi khi lưu trạng thái crawl vào {STATE_FILE}: {e}")

def clear_crawl_state_component(state: dict, component_key: str):
    """Xóa một phần của state, ví dụ khi một truyện/thể loại hoàn thành"""
    if component_key in state:
        del state[component_key]
        # Có thể cần xóa thêm các key phụ thuộc, ví dụ khi xóa genre thì xóa luôn story và chapter
        if component_key == "current_genre_url":
            state.pop("current_story_url", None)
            state.pop("current_story_index_in_genre", None)
            state.pop("processed_chapter_urls_for_current_story", None)
        elif component_key == "current_story_url":
            state.pop("processed_chapter_urls_for_current_story", None)
    save_crawl_state(state)

def clear_all_crawl_state():
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
            logger.info(f"Đã xóa file trạng thái crawl: {STATE_FILE}")
        except Exception as e:
            logger.error(f"Lỗi khi xóa file trạng thái crawl: {e}")


def sanitize_filename(name: str) -> str:
    """
    Làm sạch tên file/thư mục để tránh các ký tự không hợp lệ.
    """
    if not name:
        return "untitled"
    name = str(name)
    #Loại bỏ các ký tự đặc biệt nguy hiểm cho tên file/folder
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    # Thay thế các ký tự xuống dòng và khoảng trắng không mong muốn
    name = name.replace("\n", "").replace("\r", "")
    name = name.replace(" ", "_") # Thay khoảng trắng bằng gạch dưới
    # Loại bỏ các ký tự đặc biệt ở đầu hoặc cuối tên (., _, -)
    name = name.strip("._- ")
    # Nếu sau khi làm sạch tên rỗng, trả về "untitled"
    if not name:
        name = "untitled"
    return name[:100] # Giới hạn độ dài tên file/thư mục


def ensure_directory_exists(dir_path: str) -> bool:
    """
    Đảm bảo một thư mục tồn tại, tạo mới nếu chưa có.
    Trả về True nếu thư mục tồn tại hoặc được tạo thành công, False nếu ngược lại.
    """
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Đã tạo thư mục: {dir_path}")
            return True
        except OSError as e:
            logger.error(f"LỖI khi tạo thư mục {dir_path}: {e}")
            return False
    return True


def create_proxy_template_if_not_exists(proxies_file_path: str, proxies_folder_path: str) -> bool:
    """
    Tạo thư mục proxies và file proxies.txt mẫu nếu chúng chưa tồn tại.
    """
    if not ensure_directory_exists(proxies_folder_path):
        return False # Không thể tạo thư mục proxies

    if not os.path.exists(proxies_file_path):
        try:
            with open(proxies_file_path, "w", encoding="utf-8") as f:
                f.write("# Thêm proxy của bạn ở đây, mỗi proxy một dòng.\n")
                f.write("# Ví dụ: http://host:port\n")
                f.write("# Ví dụ: http://user:pass@host:port\n")
                f.write("# Ví dụ (định dạng IP:PORT, sẽ dùng GLOBAL credentials từ config): 123.45.67.89:1080\n")
            logger.info(f"Đã tạo file proxies mẫu: {proxies_file_path}")
            logger.info(f"Vui lòng thêm proxy vào {proxies_file_path} để sử dụng.")
            return True
        except IOError as e:
            logger.error(f"LỖI khi tạo file proxies mẫu {proxies_file_path}: {e}")
            return False
    return True

def sanitize_filename(name):
    """
    Làm sạch tên file/thư mục để tránh các ký tự không hợp lệ.
    """
    if not name:
        return "untitled"
    name = str(name)
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = name.replace("\n", "").replace("\r", "")
    name = name.replace(" ", "_")
    name = name.strip("._- ")
    if not name:
        name = "untitled"
    return name[:100]