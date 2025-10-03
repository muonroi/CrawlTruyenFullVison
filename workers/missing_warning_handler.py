import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from config.config import DATA_FOLDER
from utils.logger import logger
from workers.crawler_single_missing_chapter import crawl_single_story_worker

MISSING_WARNING_TOPIC = os.getenv("MISSING_WARNING_TOPIC", "missing_warnings")
MISSING_WARNING_GROUP = os.getenv("MISSING_WARNING_GROUP", "missing-warning-group")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_BOOTSTRAP_MAX_RETRIES = int(os.getenv("KAFKA_BOOTSTRAP_MAX_RETRIES", "0"))
KAFKA_BOOTSTRAP_RETRY_DELAY = float(os.getenv("KAFKA_BOOTSTRAP_RETRY_DELAY", "5"))


@dataclass
class MissingWarningJob:
    raw_line: str
    story_title: Optional[str]
    crawled_count: Optional[int]
    dead_count: Optional[int]
    total_count: Optional[int]

    @classmethod
    def from_dict(cls, data: dict) -> "MissingWarningJob":
        return cls(
            raw_line=str(data.get("raw_line", "")),
            story_title=data.get("story_title"),
            crawled_count=data.get("crawled_count"),
            dead_count=data.get("dead_count"),
            total_count=data.get("total_count"),
        )


async def find_story_folder(title: str) -> Optional[Path]:
    def _search() -> Optional[Path]:
        if not title:
            return None
        normalized = title.strip().casefold()
        data_path = Path(DATA_FOLDER)
        if not data_path.exists():
            return None
        for child in data_path.iterdir():
            if not child.is_dir():
                continue
            meta_path = child / "metadata.json"
            if not meta_path.exists():
                continue
            try:
                with meta_path.open("r", encoding="utf-8") as f:
                    meta = json.load(f)
            except Exception:
                continue
            meta_title = str(meta.get("title", "")).strip().casefold()
            if meta_title == normalized:
                return child
        return None

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _search)


async def process_job(job: MissingWarningJob) -> None:
    if not job.story_title:
        logger.warning(f"[MissingWarning] Cannot process job without title: {job.raw_line}")
        return
    story_folder = await find_story_folder(job.story_title)
    if not story_folder:
        logger.error(f"[MissingWarning] Story '{job.story_title}' not found in {DATA_FOLDER}")
        return

    logger.info(
        "[MissingWarning] Retrying crawl for '%s' (txt=%s dead=%s total=%s) at %s",
        job.story_title,
        job.crawled_count,
        job.dead_count,
        job.total_count,
        story_folder,
    )
    try:
        await crawl_single_story_worker(story_folder_path=str(story_folder))
        logger.info("[MissingWarning] Completed retry for '%s'", job.story_title)
    except Exception as exc:
        logger.exception(f"[MissingWarning] Failed to retry story '{job.story_title}': {exc}")


async def consume_missing_warnings() -> None:
    attempt = 0
    consumer: Optional[AIOKafkaConsumer] = None

    while True:
        attempt += 1
        consumer = AIOKafkaConsumer(
            MISSING_WARNING_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id=MISSING_WARNING_GROUP,
            session_timeout_ms=30000,
            max_poll_interval_ms=600000,
        )
        try:
            await consumer.start()
            if attempt > 1:
                logger.info(
                    "[MissingWarning] Connected to Kafka after %s attempts.", attempt
                )
            break
        except KafkaConnectionError as exc:
            logger.warning(
                "[MissingWarning] Kafka connection failed (attempt %s): %s",
                attempt,
                exc,
            )
            await consumer.stop()
            if KAFKA_BOOTSTRAP_MAX_RETRIES and attempt >= KAFKA_BOOTSTRAP_MAX_RETRIES:
                logger.error("[MissingWarning] Exceeded Kafka connection retries. Exit.")
                raise
            await asyncio.sleep(KAFKA_BOOTSTRAP_RETRY_DELAY)
        except Exception:
            await consumer.stop()
            raise

    logger.info(
        "[MissingWarning] Listening on topic '%s' (bootstrap=%s)",
        MISSING_WARNING_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )
    try:
        async for message in consumer:
            data = message.value or {}
            job = MissingWarningJob.from_dict(data)
            await process_job(job)
    except Exception as exc:
        logger.exception(f"[MissingWarning] Consumer error: {exc}")
        raise
    finally:
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume_missing_warnings())
