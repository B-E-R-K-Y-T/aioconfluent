import asyncio
from typing import Any, Optional

from confluent_kafka import Producer, KafkaException

from util.log import logger


class Publisher:
    def __init__(self, configs, push_interval: float = 0.1, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = Producer(configs)
        self._is_running = False
        self._push_interval = push_interval
        self._push_messages_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self):
        self._is_running = True
        self._push_messages_task = asyncio.create_task(self._push_loop())

    async def _push_loop(self):
        while self._is_running:
            self._producer.poll(0)

            await asyncio.sleep(self._push_interval)

    async def close(self):
        async with self._lock:
            self._is_running = True

    async def publish_force(self, topic: str, value: Any):
        result = await self.publish(topic, value)
        self._producer.poll(0)

        return result

    def publish(self, topic: str, value: Any):
        """
        An awaitable produce method.
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err)
                )
                logger.error(f"Error producing message to topic {topic}: {err}")
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)
                logger.info(f"Message produced to topic {topic}")

        self._producer.produce(topic, value, on_delivery=ack)

        return result
