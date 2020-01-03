from typing import Optional, Union, List

from typhoon.aws import dynamodb_helper
from typhoon.aws.dynamodb_helper import dynamodb_connection, DynamoDBConnectionType
from typhoon.aws.exceptions import TyphoonResourceNotFoundError
from typhoon.connections import Connection
from typhoon.core.dags import DagDeployment
from typhoon.core.metadata_store_interface import MetadataStoreInterface, MetadataObjectNotFound
from typhoon.core.settings import Settings
from typhoon.variables import Variable


class DynamodbMetadataStore(MetadataStoreInterface):
    def __init__(self, host: str, region: str, aws_profile: Optional[str] = None):
        self.host = host
        self.region = region
        self.aws_profile = aws_profile

    @property
    def client(self):

        return dynamodb_connection(
            aws_profile=self.aws_profile,
            conn_type=DynamoDBConnectionType.CLIENT,
            aws_region=self.region,
            endpoint_url=self.host,
        )

    @property
    def resource(self):
        return dynamodb_connection(
            aws_profile=self.aws_profile,
            conn_type=DynamoDBConnectionType.RESOURCE,
            aws_region=self.region,
            endpoint_url=self.host,
        )

    def close(self):
        pass

    def exists(self) -> bool:
        for table_name in [Settings.connections_table_name, Settings.variables_table_name, Settings.dag_deployments_table_name]:
            if not dynamodb_helper.dynamodb_table_exists(
                    ddb_client=self.client,
                    table_name=table_name,
            ):
                return False
        return True

    def _create_table_if_not_exists(
            self,
            table_name: str,
            primary_key: str,
            range_key: Optional[str] = None,
            read_capacity: int = 1,
            write_capacity: int = 1,
    ):
        if dynamodb_helper.dynamodb_table_exists(
                ddb_client=self.client,
                table_name=table_name,
        ):
            print(f'Table {table_name} exists. Skipping creation...')
        else:
            print(f'Creating table {table_name}...')
            kwargs = {'range_key': range_key} if range_key else {}
            dynamodb_helper.create_dynamodb_table(
                ddb_client=self.client,
                table_name=table_name,
                primary_key=primary_key,
                read_capacity_units=read_capacity,
                write_capacity_units=write_capacity,
                **kwargs
            )

    def migrate(self):
        self._create_table_if_not_exists(
            table_name=Settings.connections_table_name,
            primary_key='conn_id',
        )
        self._create_table_if_not_exists(
            table_name=Settings.variables_table_name,
            primary_key='id',
        )
        self._create_table_if_not_exists(
            table_name=Settings.dag_deployments_table_name,
            primary_key='deployment_hash',
            range_key='deployment_date',
        )

    def get_connection(self, conn_id: str) -> Connection:
        try:
            item = dynamodb_helper.dynamodb_get_item(
                ddb_client=self.client,
                table_name=Settings.connections_table_name,
                key_name='conn_id',
                key_value=conn_id,
            )
        except TyphoonResourceNotFoundError:
            raise MetadataObjectNotFound(f'Connection "{conn_id}" is not set')
        return Connection(**item)

    def get_connections(self, to_dict: bool = False) -> List[Union[dict, Connection]]:
        connections_raw = dynamodb_helper.scan_dynamodb_table(
            ddb_resource=self.resource,
            table_name=Settings.connections_table_name,
        )
        return [Connection(**conn).__dict__ if to_dict else Connection(**conn) for conn in connections_raw]

    def set_connection(self, conn: Connection):
        dynamodb_helper.dynamodb_put_item(
            ddb_client=self.client,
            table_name=Settings.connections_table_name,
            item={
                'conn_id': conn.conn_id,
                **conn.get_connection_params().__dict__,
            }
        )

    def delete_connection(self, conn: Union[str, Connection]):
        dynamodb_helper.dynamodb_delete_item(
            ddb_client=self.client,
            table_name=Settings.connections_table_name,
            key_name='conn_id',
            key_value=conn.conn_id if isinstance(conn, Connection) else conn,
        )

    def get_variable(self, variable_id: str) -> Variable:
        try:
            item = dynamodb_helper.dynamodb_get_item(
                ddb_client=self.client,
                table_name=Settings.variables_table_name,
                key_name='id',
                key_value=variable_id,
            )
        except TyphoonResourceNotFoundError:
            raise MetadataObjectNotFound(f'Variable "{variable_id}" is not set')
        return Variable(**item)

    def get_variables(self, to_dict: bool = False) -> List[Union[dict, Variable]]:
        variables_raw = dynamodb_helper.scan_dynamodb_table(
            ddb_resource=self.resource,
            table_name=Settings.variables_table_name,
        )
        return [Variable(**var).dict_contents() if to_dict else Variable(**var) for var in variables_raw]

    def set_variable(self, variable: Variable):
        dynamodb_helper.dynamodb_put_item(
            ddb_client=self.client,
            table_name=Settings.variables_table_name,
            item={
                'id': variable.id,
                **variable.dict_contents(),
            }
        )

    def delete_variable(self, variable: Union[str, Variable]):
        dynamodb_helper.dynamodb_delete_item(
            ddb_client=self.client,
            table_name=Settings.variables_table_name,
            key_name='id',
            key_value=variable.id if isinstance(variable, Variable) else variable,
        )

    def get_dag_deployment(self, deployment_hash: str) -> DagDeployment:
        try:
            item = dynamodb_helper.dynamodb_query_item(
                ddb_resource=self.resource,
                table_name=Settings.dag_deployments_table_name,
                partition_key_name='deployment_hash',
                partition_key_value=deployment_hash,
            )
        except TyphoonResourceNotFoundError:
            raise MetadataObjectNotFound(f'Deployment "{deployment_hash}" is not set')
        return DagDeployment.parse_obj(item)

    def get_dag_deployments(self, to_dict: bool = False) -> List[Union[dict, DagDeployment]]:
        dag_deployments_raw = dynamodb_helper.scan_dynamodb_table(
            ddb_resource=self.resource,
            table_name=Settings.dag_deployments_table_name,
        )
        return [DagDeployment.parse_obj(x).dict() if to_dict else DagDeployment.parse_obj(x) for x in dag_deployments_raw]

    def set_dag_deployment(self, dag_deployment: DagDeployment):
        dynamodb_helper.dynamodb_put_item(
            ddb_client=self.client,
            table_name=Settings.dag_deployments_table_name,
            item=dag_deployment.dict(),
        )
