from abc import ABC, abstractmethod

from orjson import orjson


class AbstractParser(ABC):
    @abstractmethod
    def parse(self, data: bytes) -> dict: ...


class DefaultParser(AbstractParser):
    def parse(self, data: bytes) -> dict:
        return {"body": data.decode()}

class ORjsonParser(AbstractParser):
    def parse(self, data: bytes) -> dict:
        return orjson.loads(data)
