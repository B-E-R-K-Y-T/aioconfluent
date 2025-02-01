import asyncio
from asyncio import Task
from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple, Optional, Union, TypeVar, Generic

from confluent_kafka import Consumer as ConfluentConsumer, TopicPartition, KafkaError
from pydantic import BaseModel

from util.log import logger
from util.util import run_async, run_async_thread, SerialNumberGenerator

_VM = TypeVar("_VM", bound=BaseModel)


@dataclass
class Message(Generic[_VM]):
    topic: str
    value: Union[dict, BaseModel]
    key: bytes = field(default=b"")
    offset: Optional[int] = field(default=None)
    partition: Optional[int] = field(default=None)
    timestamp: datetime = field(default=datetime.now())
    validator: _VM = field(default_factory=lambda: type(_VM))


class Subscriber(NamedTuple):
    topics: list[str]
    reader: 'Reader'


class Reader:
    def __init__(
        self,
        config: dict,
        timeout: float = .001,
        *,
        max_poll_records: int = 500,
        timeout_tasks: float = 1,
        max_queue_size: int = 10,
    ):
        self._is_running = False
        self._max_poll_records = max_poll_records
        self._consumers: Optional[list[ConfluentConsumer]] = None
        self._read_task: Optional[Task] = None
        self._config = config
        self._topics: Optional[list[str]] = None
        self._consumer = ConfluentConsumer(self._config)
        self._timeout = timeout
        self._timeout_tasks = timeout_tasks
        self._messages_queue = asyncio.Queue(maxsize=max_queue_size)
        self._lock = asyncio.Lock()
        self._name = f"Reader-{SerialNumberGenerator().counter}"

        logger.info(f"Reader '{self._name}' created with group.id {self._config['group.id']}")

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def start(self, topics: list[str]):
        self._is_running = True
        self._topics = topics
        self._consumer.subscribe(self._topics)

        self._read_task = asyncio.create_task(run_async(self._reading, args=()))
        logger.info("Reader started")

    async def get_messages(self):
        while self._is_running:
            messages = await self._messages_queue.get()
            yield (message for message in messages)

    async def commit(
        self,
        *,
        topics: Optional[list[str]] = None,
        partitions: Optional[list[int]] = None,
        asynchronous: bool = True,
    ):
        topics_partitions = None

        if all([topics, partitions]):
            topics_partitions = [
                TopicPartition(topic, partition)
                for topic, partition in zip(topics, partitions)
            ]

        if asynchronous:
            self._consumer.commit(asynchronous=asynchronous, offsets=topics_partitions)
        else:
            await run_async_thread(
                self._consumer.commit,
                args=(),
                asynchronous=asynchronous,
                offsets=topics_partitions
            )

        logger.info(f"Reader: '{self._name}' commit scheduled.")

    async def stop(self):
        logger.info("Reader stopping...")

        async with self._lock:
            self._is_running = False

        await self._wait_for_tasks()

        logger.info("Reader stopped.")

    async def _reading(self, timeout: Optional[float] = 0):
        while self._is_running:
            messages = []

            for _ in range(self._max_poll_records):
                message = self._consumer.poll(timeout=timeout)

                if message is None:
                    continue

                if message.error():
                    if message.error() == KafkaError._PARTITION_EOF:    # noqa
                        continue

                    logger.error(f"Consume error: {message.error()}")
                    continue

                logger.debug(f"Consume message: {message}")
                messages.append(message)

            if messages:
                while True:
                    try:
                        async with self._lock:
                            self._messages_queue.put_nowait(messages)

                        logger.info(
                            f"Reader: '{self._name}' consume new batch of {len(messages)} messages. "
                            f"From topics: {', '.join(self._topics)}"
                        )
                    except asyncio.QueueFull:
                        logger.debug("Queue is full. Waiting...")
                        await asyncio.sleep(.001)
                        continue
                    else:
                        break

            await asyncio.sleep(self._timeout)

    async def _wait_for_tasks(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._task_waiter(self._read_task))

    async def _task_waiter(self, task: Task):
        try:
            await asyncio.wait_for(task, timeout=self._timeout_tasks)
        except asyncio.TimeoutError:
            logger.warning(f"Task '{task}' stopped by timeout")
        except Exception as e:
            logger.error(f"Task '{task}' stopped by error: {e}")
        else:
            logger.info(f"Task '{task}' stopped by finished")

    def __aiter__(self):
        return ReadIterator(self).__aiter__()


class ReadIterator:
    def __init__(self, reader: Reader):
        self._reader = reader
        self._iterator = None

    def __aiter__(self):
        async def _agen():
            if self._iterator is None:
                raise StopAsyncIteration

            try:
                generator = await self._iterator.__anext__()

                for message in generator:
                    yield message
            except StopAsyncIteration:
                self._iterator = None
                raise

        self._iterator = self._reader.get_messages()
        self._async_generator = _agen()

        return self

    async def __anext__(self):
        return await anext(self._async_generator)
