import logging
import sys
import types

import pytest


if "aiokafka" not in sys.modules:
    aiokafka_module = types.ModuleType("aiokafka")
    errors_module = types.ModuleType("aiokafka.errors")

    class _KafkaConnectionError(Exception):
        pass

    class _StubProducer:
        async def start(self):
            pass

    errors_module.KafkaConnectionError = _KafkaConnectionError
    aiokafka_module.AIOKafkaProducer = _StubProducer
    aiokafka_module.errors = errors_module

    sys.modules["aiokafka"] = aiokafka_module
    sys.modules["aiokafka.errors"] = errors_module

from aiokafka.errors import KafkaConnectionError

from utils import kafka_producer


@pytest.fixture(autouse=True)
def reset_kafka_producer():
    kafka_producer._producer = None
    yield
    kafka_producer._producer = None


@pytest.mark.asyncio
async def test_get_kafka_producer_initializes_success(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    class DummyProducer:
        def __init__(self, *args, **kwargs):
            self.started = False

        async def start(self):
            self.started = True

    monkeypatch.setattr(kafka_producer, "AIOKafkaProducer", DummyProducer)

    producer = await kafka_producer.get_kafka_producer()

    assert isinstance(producer, DummyProducer)
    assert producer.started is True
    assert kafka_producer._producer is producer
    assert any(
        "Producer started successfully" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_get_kafka_producer_raises_connection_error(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    class FailingProducer:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise KafkaConnectionError("connection failed")

    monkeypatch.setattr(kafka_producer, "AIOKafkaProducer", FailingProducer)

    with pytest.raises(KafkaConnectionError):
        await kafka_producer.get_kafka_producer()

    assert kafka_producer._producer is None
    assert any(
        "Failed to connect and start producer" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("job", [None, {}, "not-a-dict"])
async def test_send_kafka_job_invalid_input(job, caplog):
    caplog.set_level(logging.ERROR)

    result = await kafka_producer.send_kafka_job(job)

    assert result is False
    assert any(
        "Invalid job format" in message for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_send_kafka_job_producer_unavailable(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    async def fake_get_producer():
        return None

    monkeypatch.setattr(kafka_producer, "get_kafka_producer", fake_get_producer)

    result = await kafka_producer.send_kafka_job({"type": "test"})

    assert result is False
    assert any(
        "Producer is not available" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_send_kafka_job_success(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    class DummyProducer:
        async def send_and_wait(self, topic, job):
            assert topic == kafka_producer.KAFKA_TOPIC
            assert job == {"type": "test"}

    async def fake_get_producer():
        return DummyProducer()

    monkeypatch.setattr(kafka_producer, "get_kafka_producer", fake_get_producer)

    result = await kafka_producer.send_kafka_job({"type": "test"})

    assert result is True
    assert any(
        "Job sent successfully" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_send_kafka_job_send_exception(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    class DummyProducer:
        async def send_and_wait(self, topic, job):
            raise RuntimeError("send failed")

    async def fake_get_producer():
        return DummyProducer()

    monkeypatch.setattr(kafka_producer, "get_kafka_producer", fake_get_producer)

    result = await kafka_producer.send_kafka_job({"type": "test"})

    assert result is False
    assert any(
        "Failed to send job" in message
        for message in caplog.messages
    )
