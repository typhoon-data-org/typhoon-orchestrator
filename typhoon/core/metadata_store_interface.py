from abc import ABC, abstractmethod
from typing import Union, List

from typhoon.connections import Connection
from typhoon.variables import Variable


class MetadataObjectNotFound(Exception):
    pass


class MetadataStoreInterface(ABC):
    """All implementations of the metadata store must override these functions"""
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @property
    @abstractmethod
    def uri(self) -> str:
        pass

    @abstractmethod
    def migrate(self):
        pass

    @abstractmethod
    def get_connection(self, conn_id: str) -> Connection:
        """Throws MetadataObjectNotFound"""
        pass

    @abstractmethod
    def get_connections(self, to_dict: bool = False) -> List[Union[dict, Connection]]:
        pass

    @abstractmethod
    def set_connection(self, conn: Connection):
        pass

    @abstractmethod
    def delete_connection(self, conn: Union[str, Connection]):
        pass

    @abstractmethod
    def get_variable(self, variable_id: str) -> Variable:
        """Throws MetadataObjectNotFound"""
        pass

    @abstractmethod
    def get_variables(self, to_dict: bool = False) -> List[Union[dict, Variable]]:
        pass

    @abstractmethod
    def set_variable(self, variable: Variable):
        pass

    @abstractmethod
    def delete_variable(self, variable: Union[str, Variable]):
        pass
