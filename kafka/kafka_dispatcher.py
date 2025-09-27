import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
from adapters.factory import get_adapter
from config.proxy_provider import shuffle_proxies
from utils.logger import logger
from core.config_loader import apply_env_overrides
from workers.crawler_missing_chapter import loop_once_multi_sites
from workers.crawler_single_missing_chapter import crawl_single_story_worker
from main import WorkerSettings, retry_failed_genres, run_single_site, run_all_sites
from workers.retry_failed_chapters import retry_single_chapter

# ==== Config Kafka ====
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crawl_truyen")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "novel-crawler-group")

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
            genre_batch_size=int(os.getenv("GENRE_BATCH_SIZE", 3)),
            genre_async_limit=int(os.getenv("GENRE_ASYNC_LIMIT", 3)),
            proxies_file=os.getenv("PROXIES_FILE", "proxies/proxies.txt"),
            failed_genres_file=os.getenv("FAILED_GENRES_FILE", "failed_genres.json"),
            retry_genre_round_limit=int(os.getenv("RETRY_GENRE_ROUND_LIMIT", 3)),
            retry_sleep_seconds=int(os.getenv("RETRY_SLEEP_SECONDS", 1800)),
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
    logger.info(f"[Kafka] 🔌 Kết nối đến Kafka tại {KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC}")
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=KAFKA_GROUP_ID,
    )
    await consumer.start()

    try:
        logger.info(f"[Kafka] Đang lắng nghe jobs trên topic `{KAFKA_TOPIC}`...")
        async for msg in consumer:
            job = msg.value
            await dispatch_job(job) #type: ignore
    except Exception as ex:
        logger.exception(f"[Kafka] Lỗi toàn cục trong consumer: {ex}")
    finally:
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
