from typhoon.connections import Connection
from typhoon.variables import Variable


class MetadataStoreInterface:
    def get_connection(self, conn_id: str) -> Connection:
        pass

    def set_connection(self, conn: Connection):
        pass

    def get_variable(self, variable_id: str) -> Variable:
        pass

    def set_variable(self, variable: Variable):
        pass
