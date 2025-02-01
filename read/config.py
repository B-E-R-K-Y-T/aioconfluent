from typing import Literal, Union, Optional

from pydantic import BaseModel, Field

from util.util import gen_unique_id


class ReaderConfig(BaseModel):
    bootstrap_servers: Union[str, list[str]] = Field(default="localhost:9092")  # Адрес(а) Kafka брокера(ов)
    group_id: str = Field(default=f"aio-confluent-group-{gen_unique_id()}")  # Идентификатор группы потребителей
    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(default="earliest")  # Положение смещения, если смещение не найдено
    enable_auto_commit: bool = Field(default=True)  # Включить автоматическое подтверждение смещения
    auto_commit_interval_ms: int = Field(default=5000)  # Интервал автоподтверждения смещения (в миллисекундах)
    session_timeout_ms: int = Field(default=30000)  # Время ожидания, после которого группа считается недействительной
    max_poll_records: int = Field(default=500)  # Максимальное количество записей, возвращаемых при вызове poll()
    max_partition_fetch_bytes: int = Field(default=1048576)  # Максимальный размер данных, полученных из одной партиции (в байтах)
    max_poll_interval_ms: int = Field(default=60000)  # Максимальное время между вызовами poll()
    client_id: Optional[str] = Field(default=None)  # Идентификатор клиента
    isolation_level: Literal["read_uncommitted", "read_committed"] = Field(default="read_uncommitted")  # Уровень изоляции
    fetch_min_bytes: int = Field(default=1)  # Минимальный объем данных, при необходимости, который должен быть возвращен


class PublisherConfig(BaseModel):
    bootstrap_servers: Union[str, list[str]] = Field(default="localhost:9092")
    client_id: Optional[str] = Field(default=None)  # Идентификатор клиента
    acks: Literal["0", "1", "all"] = Field(default="1")  # Параметр подтверждения
    retries: int = Field(default=0)  # Количество попыток повторной отправки
    batch_size: int = Field(default=16384)  # Размер пакета в байтах
    linger_ms: int = Field(default=0)  # Время ожидания перед отправкой сообщения
    buffer_memory: int = Field(default=33554432)  # Объем памяти для буфера
    compression_type: Optional[Literal["none", "gzip", "snappy", "lz4", "zstd"]] = Field(default="none")  # Тип сжатия
    max_in_flight_requests_per_connection: int = Field(default=5)  # Максимальное количество запросов
    security_protocol: Optional[Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]] = Field(default="plaintext")  # ПNamedTupleзопасности


class KafkaConfig(BaseModel):
    reader: ReaderConfig = Field(default=ReaderConfig())
    publisher: PublisherConfig = Field(default=PublisherConfig())
