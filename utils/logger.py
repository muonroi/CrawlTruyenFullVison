from loguru import logger
import os

LOG_FOLDER = "logs"
os.makedirs(LOG_FOLDER, exist_ok=True)

logger.remove()  # Xóa default handler
logger.add(
    os.path.join(LOG_FOLDER, "crawl_{time:YYYY-MM-DD}.log"),
    rotation="1 day",
    retention="7 days",
    level="INFO",
    encoding="utf-8"
)