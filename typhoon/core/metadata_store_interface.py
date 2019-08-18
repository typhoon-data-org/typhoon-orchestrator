from abc import ABC, abstractmethod

from typhoon.config import TyphoonConfig
from typhoon.connections import Connection
from typhoon.variables import Variable


class MetadataStoreNotInitializedError(Exception):
    pass


class MetadataStoreInterface(ABC):
    """All implementations of the metadata store must override these functions"""
    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def migrate(self, config: TyphoonConfig):
        pass

    @abstractmethod
    def get_connection(self, conn_id: str) -> Connection:
        pass

    @abstractmethod
    def set_connection(self, conn: Connection):
        pass

    @abstractmethod
    def get_variable(self, variable_id: str) -> Variable:
        pass

    @abstractmethod
    def set_variable(self, variable: Variable):
        pass
