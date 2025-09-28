
import asyncio
import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from utils.logger import logger
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

_producer = None
_producer_lock = asyncio.Lock()

async def get_kafka_producer():
    """
    Initializes and returns a singleton AIOKafkaProducer instance.

    Uses a lock to ensure that only one producer instance is created.
    If the producer is already initialized, it returns the existing instance.
    """
    global _producer
    async with _producer_lock:
        if _producer is None:
            logger.info(f"[Kafka Producer] Initializing producer for brokers: {KAFKA_BOOTSTRAP_SERVERS}")
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            try:
                await producer.start()
                _producer = producer
                logger.info("[Kafka Producer] Producer started successfully.")
            except KafkaConnectionError as e:
                logger.error(f"[Kafka Producer] Failed to connect and start producer: {e}")
                raise
        return _producer

async def send_kafka_job(job: dict):
    """
    Sends a job to the configured Kafka topic.

    It ensures the producer is initialized, then sends the job dictionary
    as a JSON-serialized message.

    Args:
        job (dict): The job payload to send.

    Returns:
        bool: True if the job was sent successfully, False otherwise.
    """
    if not job or not isinstance(job, dict):
        logger.error("[Kafka Producer] Invalid job format. Job must be a non-empty dictionary.")
        return False

    try:
        producer = await get_kafka_producer()
        if not producer:
            logger.error("[Kafka Producer] Producer is not available. Cannot send job.")
            return False

        logger.info(f"[Kafka Producer] Sending job to topic '{KAFKA_TOPIC}': {job}")
        await producer.send_and_wait(KAFKA_TOPIC, job)
        logger.info(f"[Kafka Producer] Job sent successfully: {job.get('type')}")
        return True
    except Exception as e:
        logger.exception(f"[Kafka Producer] Failed to send job: {e}")
        return False

async def stop_kafka_producer():
    """
    Stops the singleton Kafka producer instance if it is running.
    """
    global _producer
    async with _producer_lock:
        if _producer:
            logger.info("[Kafka Producer] Stopping producer...")
            await _producer.stop()
            _producer = None
            logger.info("[Kafka Producer] Producer stopped.")

if __name__ == "__main__":
    async def test_send_job():
        test_job = {"type": "test_job", "data": "Hello Kafka from producer!"}
        success = await send_kafka_job(test_job)
        if success:
            print("Test job sent successfully.")
        else:
            print("Failed to send test job.")
        await stop_kafka_producer()

    asyncio.run(test_send_job())
