
import json
import asyncio
import os
from aiokafka import AIOKafkaProducer
from utils.logger import logger

# Sử dụng chung cấu hình với consumer
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crawl_truyen")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

_producer = None
_producer_lock = asyncio.Lock()

async def get_producer():
    """
    Khởi tạo và trả về một producer duy nhất (singleton).
    """
    global _producer
    async with _producer_lock:
        if _producer is None or _producer._closed:
            logger.info(f"[Kafka Producer] Đang khởi tạo producer tới {KAFKA_BOOTSTRAP_SERVERS}...")
            _producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            await _producer.start()
            logger.info("[Kafka Producer] Producer đã sẵn sàng.")
    return _producer

async def send_job(job_data: dict, topic: str = None):
    """
    Gửi một job (dictionary) tới topic Kafka được chỉ định.

    Args:
        job_data: Dữ liệu của job dưới dạng dictionary.
        topic: Tên topic để gửi. Nếu là None, sẽ dùng topic mặc định.
    """
    target_topic = topic or KAFKA_TOPIC
    try:
        producer = await get_producer()
        await producer.send_and_wait(target_topic, job_data)
        job_type = job_data.get("type", "unknown")
        logger.info(f"[Kafka Producer] ✅ Đã gửi job '{job_type}' tới topic '{target_topic}'.")
    except Exception as e:
        logger.exception(f"[Kafka Producer] ❌ Lỗi khi gửi job tới topic '{target_topic}': {e}")
        # Nếu có lỗi, thử đóng producer cũ để lần sau khởi tạo lại
        global _producer
        if _producer:
            await _producer.stop()
            _producer = None

async def close_producer():
    """
    Đóng producer khi ứng dụng kết thúc.
    """
    global _producer
    if _producer:
        logger.info("[Kafka Producer] Đang đóng producer...")
        await _producer.stop()
        _producer = None
        logger.info("[Kafka Producer] Đã đóng producer.")

