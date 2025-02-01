from typing import TypedDict, Literal, Union, Callable


class ConsumerConfig(TypedDict):
    bootstrap_servers: Union[str, list]
    group_id: str
    auto_offset_reset: Literal["earliest", "latest", "none"]
    enable_auto_commit: bool
    auto_commit_interval_ms: int
    session_timeout_ms: int
    max_poll_records: int
    max_partition_fetch_bytes: int
    max_poll_interval_ms: int
    fetch_max_wait_ms: int
    fetch_min_bytes: int
    value_deserializer: Union[str, Callable]
    key_deserializer: Union[str, Callable]
