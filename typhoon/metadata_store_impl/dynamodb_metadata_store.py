from typing import Optional, Union, List

from typhoon.aws.plumbing import dynamodb_plumbing
from typhoon.aws.plumbing.dynamodb_plumbing import dynamodb_connection, DynamoDBConnectionType
from typhoon.connections import Connection
from typhoon.core.metadata_store_interface import MetadataStoreInterface
from typhoon.variables import Variable


class DynamodbMetadataStore(MetadataStoreInterface):
    def __init__(self, config: Optional['TyphoonConfig'] = None):
        from typhoon.core.config import TyphoonConfig

        self.config = config or TyphoonConfig()

    @property
    def client(self):
        from typhoon.core.config import CLIConfig
        if isinstance(self.config, CLIConfig):
            aws_profile = self.config.aws_profile
        else:
            aws_profile = None
        return dynamodb_connection(
            aws_profile=aws_profile,
            conn_type=DynamoDBConnectionType.CLIENT,
            aws_region=self.config.dynamodb_region,
            endpoint_url=self.config.dynamodb_endpoint,
        )

    @property
    def resource(self):
        from typhoon.core.config import CLIConfig
        if isinstance(self.config, CLIConfig):
            aws_profile = self.config.aws_profile
        else:
            aws_profile = None
        return dynamodb_connection(
            aws_profile=aws_profile,
            conn_type=DynamoDBConnectionType.RESOURCE,
            aws_region=self.config.dynamodb_region,
            endpoint_url=self.config.dynamodb_endpoint,
        )

    def close(self):
        pass

    def migrate(self):
        pass

    def get_connection(self, conn_id: str) -> Connection:
        item = dynamodb_plumbing.dynamodb_get_item(
            ddb_client=self.client,
            table_name=self.config.connections_table_name,
            key_name='conn_id',
            key_value=conn_id,
        )
        return Connection(**item)

    def get_connections(self, to_dict: bool = False) -> List[Union[dict, Connection]]:
        connections_raw = dynamodb_plumbing.scan_dynamodb_table(
            ddb_resource=self.resource,
            table_name=self.config.connections_table_name,
        )
        return [Connection(**conn).__dict__ if to_dict else Connection(**conn) for conn in connections_raw]

    def set_connection(self, conn: Connection):
        dynamodb_plumbing.dynamodb_put_item(
            ddb_client=self.client,
            table_name=self.config.connections_table_name,
            item={
                'conn_id': conn.conn_id,
                **conn.get_connection_params().__dict__,
            }
        )

    def delete_connection(self, conn: Union[str, Connection]):
        dynamodb_plumbing.dynamodb_delete_item(
            ddb_client=self.client,
            table_name=self.config.connections_table_name,
            key_name='conn_id',
            key_value=conn.conn_id if isinstance(conn, Connection) else conn,
        )

    def get_variable(self, variable_id: str) -> Variable:
        item = dynamodb_plumbing.dynamodb_get_item(
            ddb_client=self.client,
            table_name=self.config.variables_table_name,
            key_name='id',
            key_value=variable_id,
        )
        return Variable(**item)

    def get_variables(self, to_dict: bool = False) -> List[Union[dict, Variable]]:
        variables_raw = dynamodb_plumbing.scan_dynamodb_table(
            ddb_resource=self.resource,
            table_name=self.config.variables_table_name,
        )
        return [Variable(**var).dict_contents() if to_dict else Variable(**var) for var in variables_raw]

    def set_variable(self, variable: Variable):
        dynamodb_plumbing.dynamodb_put_item(
            ddb_client=self.client,
            table_name=self.config.variables_table_name,
            item={
                'id': variable.id,
                **variable.dict_contents(),
            }
        )

    def delete_variable(self, variable: Union[str, Variable]):
        dynamodb_plumbing.dynamodb_delete_item(
            ddb_client=self.client,
            table_name=self.config.variables_table_name,
            key_name='id',
            key_value=variable.id if isinstance(variable, Variable) else variable,
        )
