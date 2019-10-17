from abc import ABC, abstractmethod

from typhoon.connections import Connection
from typhoon.variables import Variable


class MetadataStoreInterface(ABC):
    """All implementations of the metadata store must override these functions"""
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def migrate(self):
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
