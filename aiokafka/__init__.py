"""Minimal stub of :mod:`aiokafka` providing :class:`AIOKafkaProducer`."""

from __future__ import annotations


class AIOKafkaProducer:
    def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - trivial stub
        pass

    async def start(self) -> None:  # pragma: no cover
        return None

    async def stop(self) -> None:  # pragma: no cover
        return None

    async def send_and_wait(self, topic: str, value) -> None:  # pragma: no cover
        return None


__all__ = ["AIOKafkaProducer"]
