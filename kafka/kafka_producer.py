
import json
import asyncio
import importlib.util
from config import config as app_config
from utils.logger import logger

try:
    _AIOKAFKA_SPEC = importlib.util.find_spec("aiokafka")
except ModuleNotFoundError:  # pragma: no cover - optional dependency missing
    _AIOKAFKA_SPEC = None

if _AIOKAFKA_SPEC:
    from aiokafka import AIOKafkaProducer  # type: ignore
else:
    AIOKafkaProducer = None  # type: ignore

_producer = None
_producer_lock = asyncio.Lock()


class _DummyProducer:
    def __init__(self) -> None:
        self._closed = False

    async def start(self) -> None:  # pragma: no cover - simple fallback
        self._closed = False

    async def stop(self) -> None:  # pragma: no cover
        self._closed = True

    async def send_and_wait(self, topic, payload) -> None:  # pragma: no cover
        logger.info("[Kafka Dummy] would send to %s: %s", topic, payload)

async def get_producer():
    """
    Khởi tạo và trả về một producer duy nhất (singleton).
    """
    global _producer
    async with _producer_lock:
        if _producer is None or getattr(_producer, "_closed", False):
            if AIOKafkaProducer is None:
                logger.warning("aiokafka not installed; using in-memory dummy producer")
                _producer = _DummyProducer()
            else:
                logger.info(f"[Kafka Producer] Đang khởi tạo producer tới {app_config.KAFKA_BOOTSTRAP_SERVERS}...")
                _producer = AIOKafkaProducer(
                    bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS,
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
    target_topic = topic or app_config.KAFKA_TOPIC
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

