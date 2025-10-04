import asyncio
import json
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from adapters.factory import get_adapter
from config.proxy_provider import shuffle_proxies
from utils.logger import logger
from core.config_loader import apply_env_overrides
from workers.crawler_missing_chapter import loop_once_multi_sites
from workers.crawler_single_missing_chapter import crawl_single_story_worker
from main import WorkerSettings, retry_failed_genres, run_single_site, run_all_sites
from workers.retry_failed_chapters import retry_single_chapter

from config import config as app_config

# ==== Job Dispatcher Mapping ====
from main import run_single_story

WORKER_HANDLERS = {
    "single_story": lambda **job: run_single_story(
        title=job["title"],
        site_key=job.get("site_key"),
        genre_name=job.get("genre_name"),
    ),
    "missing_check": loop_once_multi_sites,
    "healthcheck": lambda **job: healthcheck_adapter(site_key=job["site_key"]),
    "retry_chapter": lambda **job: retry_single_chapter(job),
    "check_missing_chapters": lambda **job: crawl_single_story_worker(story_folder_path=job.get("story_folder_path")),
}


async def dispatch_job(job: dict):
    apply_env_overrides(job)
    job_type = job.get("type")

    if not job_type:
        logger.warning("[Kafka] Không tìm thấy type trong message.")
        return

    if job_type == "retry_failed_genres":
        site_key = job.get("site_key")
        if not site_key:
            logger.error("[Kafka] Thiếu `site_key` trong job `retry_failed_genres`.")
            return

        settings = WorkerSettings(
            genre_batch_size=app_config.GENRE_BATCH_SIZE,
            genre_async_limit=app_config.GENRE_ASYNC_LIMIT,
            proxies_file=app_config.PROXIES_FILE,
            failed_genres_file=app_config.FAILED_GENRES_FILE,
            retry_genre_round_limit=app_config.RETRY_GENRE_ROUND_LIMIT,
            retry_sleep_seconds=app_config.RETRY_SLEEP_SECONDS,
        )
        await retry_failed_genres(get_adapter(site_key), site_key, settings, shuffle_proxies)
        return

    elif job_type == "full_site":
        site_key = job.get("site_key")
        crawl_mode = job.get("crawl_mode")
        if not site_key:
            logger.error("[Kafka] Thiếu `site_key` trong job `full_site`.")
            return
        await run_single_site(site_key=site_key, crawl_mode=crawl_mode)
        return

    elif job_type == "all_sites":
        crawl_mode = job.get("crawl_mode")
        await run_all_sites(crawl_mode=crawl_mode)
        return

    handler = WORKER_HANDLERS.get(job_type)
    if not handler:
        logger.error(f"[Kafka] Không hỗ trợ job type: {job_type}")
        return

    logger.info(f"[Kafka] 🔧 Đang xử lý job `{job_type}` với data: {job}")
    try:
        await handler(**job)
    except TypeError as te:
        logger.error(f"[DISPATCH] Lỗi gọi hàm `{handler.__name__}` với kwargs: {te}")
    except Exception as ex:
        logger.exception(f"[Kafka] ❌ Lỗi khi xử lý job `{job_type}`: {ex}")

async def consume():
    logger.info(
        f"[Kafka] 🔌 Kết nối đến Kafka tại {app_config.KAFKA_BOOTSTRAP_SERVERS} | topic={app_config.KAFKA_TOPIC}"
    )
    max_retries = app_config.KAFKA_BOOTSTRAP_MAX_RETRIES
    retry_delay = app_config.KAFKA_BOOTSTRAP_RETRY_DELAY
    attempt = 0
    consumer = None

    while True:
        attempt += 1
        consumer = AIOKafkaConsumer(
            app_config.KAFKA_TOPIC,
            bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id=app_config.KAFKA_GROUP_ID,
            session_timeout_ms=30000,
            max_poll_interval_ms=600000,  # 10 phút
        )
        try:
            await consumer.start()
            if attempt > 1:
                logger.info(f"[Kafka] Kết nối Kafka thành công sau {attempt} lần thử.")
            break
        except KafkaConnectionError as ex:
            logger.warning(f"[Kafka] Không kết nối được Kafka (attempt {attempt}): {ex}")
            await consumer.stop()
            if max_retries and attempt >= max_retries:
                logger.error("[Kafka] Vượt quá số lần retry kết nối Kafka. Thử lại sau.")
                raise
            await asyncio.sleep(retry_delay)
        except Exception:
            await consumer.stop()
            raise

    try:
        logger.info(f"[Kafka] Đang lắng nghe jobs trên topic `{app_config.KAFKA_TOPIC}`...")
        async for msg in consumer:
            job = msg.value
            await dispatch_job(job)  # type: ignore
    except Exception as ex:
        logger.exception(f"[Kafka] Lỗi toàn cục trong consumer: {ex}")
    finally:
        if consumer:
            await consumer.stop()
        logger.info("[Kafka] Đã dừng consumer.")



async def healthcheck_adapter(site_key: str):
    adapter = get_adapter(site_key)
    try:
        genres = await adapter.get_genres()
        if not genres or len(genres) == 0:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy genres FAIL hoặc rỗng!")
            return False
        genre = genres[0]
        stories = await adapter.get_stories_in_genre(genre['title'],genre['url'], )
        if not stories or len(stories) == 0:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy stories FAIL hoặc rỗng!")
            return False
        story = stories[0]
        details = await adapter.get_story_details(story['url'], story['title']) #type: ignore
        if not details or "title" not in details:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy details FAIL hoặc thiếu field!")
            return False
        chapters = await adapter.get_chapter_list(story['url'], story['title'], site_key)#type: ignore
        if not chapters or len(chapters) == 0:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy chapters FAIL hoặc rỗng!")
            return False
        chap = chapters[0]
        content = await adapter.get_chapter_content(chap['url'], chap['title'], site_key)
        if not content or len(content) < 50: #type: ignore
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy nội dung chương FAIL hoặc rỗng!")
            return False
        logger.info(f"[HEALTHCHECK] {site_key}: OK")
        return True
    except Exception as ex:
        logger.error(f"[HEALTHCHECK] {site_key}: Exception: {ex}")
        return False

if __name__ == "__main__":
    asyncio.run(consume())
