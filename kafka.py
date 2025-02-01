import asyncio
import inspect
from asyncio import Task
from itertools import zip_longest
from typing import Union, Callable, Optional, Coroutine, Type

from pydantic import BaseModel, ValidationError

from exceptions import BackgroundProcessingException
from read.config import KafkaConfig
from util.handler import Handler, get_function_metadata
from util.parse import AbstractParser, DefaultParser
from publish.publisher import Publisher
from read.reader import Subscriber, Reader, Message
from util.log import logger
from util.util import gen_unique_id, run_async_thread


class Kafka:
    def __init__(
            self,
            kafka_config: KafkaConfig = KafkaConfig(),
            *,
            name: str = f"aio-confluent-kafka-client-{gen_unique_id()}",
            order_processing: bool = False,
            default_parser: Type[AbstractParser] = DefaultParser,
    ):
        self._kafka_config = kafka_config
        self._publisher_config = {
            "bootstrap.servers": self._kafka_config.publisher.bootstrap_servers,
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
    def config(self) -> KafkaConfig:
        return self._kafka_config

    @property
    def name(self) -> str:
        return self._name

    async def publish(self, message: Message):
        return await self._publisher.publish(message.topic, message.value)

    async def create_publisher(self, bootstrap_servers: Optional[Union[str, list]] = None) -> Publisher:
        if bootstrap_servers is None:
            bootstrap_servers = self._kafka_config.bootstrap_servers

        return Publisher(bootstrap_servers)

    def subscribe(self, topics: list[str]) -> Subscriber:
        subscriber = Subscriber(
            topics=topics,
            reader=Reader(
                self._kafka_config.reader
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
        logger.info(f"Kafka: {self._kafka_config.reader.bootstrap_servers}")

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
    def _validate(handler: Handler, consume_message: Message, handler_annotations) -> BaseModel:
        validator: Type[BaseModel] = handler_annotations["value"]

        if not isinstance(validator, type(BaseModel)):
            raise ValueError(
                f"Handler {handler.handler.__name__} must have 'value' argument of type BaseModel"
            )

        return validator(**consume_message.value)

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

                if handler.signature.annotations:
                    if handler.signature.annotations["value"] is not inspect._empty: # noqa
                        try:
                            consume_message.value = self._validate(handler, consume_message, handler.signature.annotations)
                            value = consume_message.value
                        except ValidationError as e:
                            logger.error(e)
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
                for handlers in zip_longest(*topics_handlers, fillvalue=None): # noqa
                    coroutines = [handler for handler in handlers if handler is not None]
                    await self._start_handlers_coroutine(*coroutines)

            else:
                for handlers in topics_handlers:
                    self._background_processing_tasks.append(
                        asyncio.create_task(self._start_handlers_coroutine(*handlers))
                    )
