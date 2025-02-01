from abc import ABC, abstractmethod


class AbstractParser(ABC):
    @abstractmethod
    def parse(self, data: bytes) -> dict: ...


class DefaultParser(AbstractParser):
    def parse(self, data: bytes) -> dict:
        return {"value": data.decode()}
