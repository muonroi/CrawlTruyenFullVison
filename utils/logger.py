import os
import logging
from logging.handlers import RotatingFileHandler

# === Đảm bảo tồn tại thư mục logs/
LOG_FOLDER = "logs"
os.makedirs(LOG_FOLDER, exist_ok=True)

# === Đường dẫn file log chính
LOG_FILE_PATH = os.path.join(LOG_FOLDER, "crawler.log")

# === Khởi tạo logger
logger = logging.getLogger("StoryFlowLogger")
logger.setLevel(logging.DEBUG)

# === Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# === File handler với xoay vòng log (5MB, lưu 3 file backup)
fh = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
fh.setLevel(logging.DEBUG)

# === Định dạng log
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# === Gắn handler
logger.addHandler(ch)
logger.addHandler(fh)
