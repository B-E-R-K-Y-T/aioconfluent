import inspect
from typing import Callable, NamedTuple, Any

from read.reader import Subscriber, Message


class HandlerSignature(NamedTuple):
    value: Any
    message: Any
    annotations: dict


class Handler(NamedTuple):
    handler: Callable
    subscriber: Subscriber
    signature: HandlerSignature


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

    return HandlerSignature(
        value=value,
        message=message,
        annotations=annotations,
    )
