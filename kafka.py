import asyncio
import inspect
from asyncio import Task
from itertools import zip_longest
from typing import Union, Literal, Callable, NamedTuple, Optional, Coroutine, Type, Any

from pydantic import BaseModel, ValidationError

from exceptions import BackgroundProcessingException
from util.parse import AbstractParser, DefaultParser
from publish.publisher import Publisher
from read.reader import Subscriber, Reader, Message
from util.log import logger
from util.util import gen_unique_id, run_async_thread


class HandlerMetadata(NamedTuple):
    value: Any
    message: Any
    annotations: dict


class Handler(NamedTuple):
    handler: Callable
    subscriber: Subscriber
    metadata: HandlerMetadata


class Kafka:
    def __init__(
            self,
            bootstrap_servers: Union[str, list] = "localhost:9092",
            group_id: str = f"aio-confluent-group-{gen_unique_id()}",
            auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest",
            enable_auto_commit: bool = True,
            auto_commit_interval_ms: int = 5000,
            session_timeout_ms: int = 30000,
            max_poll_records: int = 500,
            max_partition_fetch_bytes: int = 1048576,
            max_poll_interval_ms: int = 60000,
            fetch_min_bytes: int = 1,
            *,
            name: str = f"aio-confluent-kafka-client-{gen_unique_id()}",
            order_processing: bool = False,
            default_parser: Type[AbstractParser] = DefaultParser,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._session_timeout_ms = session_timeout_ms
        self._max_poll_records = max_poll_records
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._max_poll_interval_ms = max_poll_interval_ms
        self._fetch_min_bytes = fetch_min_bytes

        self._consumer_config = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": self._group_id,
            "auto.offset.reset": self._auto_offset_reset,
            "enable.auto.commit": self._enable_auto_commit,
            "auto.commit.interval.ms": self._auto_commit_interval_ms,
            "session.timeout.ms": self._session_timeout_ms,
            "max.partition.fetch.bytes": self._max_partition_fetch_bytes,
            "max.poll.interval.ms": self._max_poll_interval_ms,
            "fetch.min.bytes": self._fetch_min_bytes,
        }
        self._publisher_config = {
            "bootstrap.servers": self._bootstrap_servers,
        }

        self._consumers: list[Subscriber] = []
        self._handlers: list[Handler] = []
        self._publisher: Optional[Publisher] = None
        self._is_running = False
        self._background_processing_tasks: list[Task] = []
        self.order_processing = order_processing
        self._name = name
        self._default_parser = default_parser

    @property
    def name(self):
        return self._name

    async def publish(self, message: Message):
        return await self._publisher.publish(message.topic, message.value)

    async def create_publisher(self, bootstrap_servers: Optional[Union[str, list]] = None) -> Publisher:
        if bootstrap_servers is None:
            bootstrap_servers = self._bootstrap_servers

        return Publisher(bootstrap_servers)

    def subscribe(self, topics: list[str]) -> Subscriber:
        subscriber = Subscriber(
            topics=topics,
            reader=Reader(
                self._consumer_config, max_poll_records=self._max_poll_records
            ),
        )

        if topics in [c.topics for c in self._consumers]:
            raise ValueError(f"Subscriber for {topics} already exists")

        self._consumers.append(subscriber)

        return subscriber

    def handler(self, topics: list[str]):
        logger.info(f"Bind: {topics}")
        subscriber = self.subscribe(topics)

        def decorator(func: Callable):
            self._handlers.append(Handler(func, subscriber, get_function_metadata(func)))
            return func

        return decorator

    async def run(self):
        logger.info(f"Kafka: {self._bootstrap_servers}")

        self._is_running = True
        self._publisher = Publisher(self._publisher_config)
        await self._publisher.start()

        async with asyncio.TaskGroup() as tg:
            for handler in self._handlers:
                logger.info(f"Start readers for: {handler.subscriber.topics}")
                tg.create_task(
                    handler.subscriber.reader.start(handler.subscriber.topics)
                )

        logger.info(f"Wait for messages...")
        await self._listen()

    async def _listen(self):
        async with asyncio.TaskGroup() as tg:
            for handler in self._handlers:
                tg.create_task(self._runner(handler))

    @staticmethod
    async def _start_handlers_coroutine(*handlers: Coroutine):
        try:
            async with asyncio.TaskGroup() as tg:
                for coro in handlers:
                    tg.create_task(coro)
        except* Exception as e:
            logger.error(f"Error: {e}")
            raise BackgroundProcessingException(e) from e

    @staticmethod
    def _validate(handler: Handler, consume_message: Message, handler_annotations) -> bool:
        validator: Type[BaseModel] = handler_annotations["value"]

        if not isinstance(validator, type(BaseModel)):
            raise ValueError(
                f"Handler {handler.handler.__name__} must have 'value' argument of type BaseModel"
            )

        try:
            consume_message.value = validator(**consume_message.value)
        except ValidationError as e:
            logger.error(e)
            return False

        return True

    async def _runner(self, handler: Handler):
        while self._is_running:
            coro_handlers: dict[str, list[Coroutine]] = {}

            async for message in handler.subscriber.reader:
                topic = message.topic()

                if topic not in coro_handlers:
                    coro_handlers[topic] = []

                value = self._default_parser().parse(message.value())
                consume_message = Message(
                    topic=message.topic(),
                    value=value,
                    offset=message.offset(),
                    partition=message.partition(),
                    key=message.key(),
                    timestamp=message.timestamp(),
                )

                if handler.metadata.annotations:
                    if handler.metadata.annotations["value"] is not inspect._empty:
                        if not self._validate(handler, consume_message, handler.metadata.annotations):
                            continue

                coro_handlers[topic].append(
                    run_async_thread(
                        handler.handler,
                        args=(value, consume_message),
                    )
                )

            topics_handlers = []

            for handlers in coro_handlers.values():
                topics_handlers.append(handlers)

            if self.order_processing:
                for handlers in zip_longest(*topics_handlers, fillvalue=None):
                    coroutines = [handler for handler in handlers if handler is not None]
                    await self._start_handlers_coroutine(*coroutines)

            else:
                for handlers_ in topics_handlers:
                    self._background_processing_tasks.append(
                        asyncio.create_task(self._start_handlers_coroutine(*handlers_))
                    )


def get_function_metadata(handler: Callable):
    signature = inspect.signature(handler)
    value = None
    message = None
    annotations = {}

    for param in signature.parameters.values():
        if param.name == "value":
            value = param
        elif param.name == "message":
            message = param

        annotations[param.name] = param.annotation

    if value is None:
        raise ValueError(f"Handler {handler.__name__} must have 'value' argument")

    if message is None:
        raise ValueError(f"Handler {handler.__name__} must have 'message' argument")

    if message.annotation is not Message:
        raise ValueError(f"Handler {handler.__name__} must have 'message' argument of type Message")

    return HandlerMetadata(
        value=value,
        message=message,
        annotations=annotations,
    )
