import hashlib
import re
from datetime import datetime
from enum import Enum
from typing import List, Union, Dict, Any, Optional

from pydantic import BaseModel, validator, Field, root_validator

IDENTIFIER_REGEX = r'\w+'
Identifier = Field(..., regex=r'\w+')
Item = Union[int, str, float, Dict, List]


class SpecialCronString(str, Enum):
    daily = '@daily'
    weekly = '@weekly'
    monthly = '@monthly'
    yearly = '@yearly'


class Node(BaseModel):
    function: str = Field(
        ...,
        regex=r'(typhoon\.\w+\.\w+|functions\.\w+\.\w+)',
        description="""Python function that will get called when the node runs.
                    If it is a built-in typhoon function it will have the following structure:
                      typhoon.[MODULE_NAME].[FUNCTION_NAME]
                    Whereas if it is a user defined function it will have the following structure:
                      functions.[MODULE_NAME].[FUNCTION_NAME]"""
    )
    asynchronous: bool = Field(
        True,
        description="""If set to TRUE it will run the function in a different lambda instance.
                    This is useful when you want to increase parallelism. There is currently no framework cap on
                    parallelism though so if that is an issue set it to FALSE so it will run the batches one by one."""
    )
    config: Dict[str, Any] = Field(default={})

    @validator('config')
    def validate_config_keys(cls, v):
        for k in v.keys():
            if not re.match(r'\w+(\s*=>\s*APPLY)?', k):
                raise ValueError(f'Config key "{k}" does not match pattern. Must be identifier with optional => APPLY')
        return v


class Edge(BaseModel):
    source: str = Field(..., regex=IDENTIFIER_REGEX, description='ID of source node')
    adapter: Dict[str, Item] = Field(
        ...,
        description='Adapts the output of the source node to the input of the destination node'
    )
    destination: str = Field(..., regex=IDENTIFIER_REGEX, description='ID of destination node')

    @validator('adapter')
    def validate_adapter_keys(cls, v):
        for k in v.keys():
            if not re.match(r'\w+(\s*=>\s*APPLY)?', k):
                raise ValueError(f'Config key "{k}" does not match pattern. Must be identifier with optional => APPLY')
        return v


class DAG(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your DAG')
    schedule_interval: str = Field(
        ...,
        regex='(' + '@hourly|@daily|@weekly|@monthly|@yearly|' +
              r'((\*|\?|\d+((\/|\-){0,1}(\d+))*)\s*){5,6}' + '|' +
              r'rate\(\s*1\s+minute\s*\)' + '|' +
              r'rate\(\s*\d+\s+minutes\s*\)' + '|' +
              r'rate\(\s1*\d+\s+hour\s*\)' + '|' +
              r'rate\(\s*\d+\s+hours\s*\)' + '|' +
              ')',
        description='Schedule or frequency on which the DAG should run'
    )
    nodes: Dict[str, Node]
    edges: Dict[str, Edge]
    active: bool = Field(True, description='Whether to deploy the DAG or not')

    @root_validator
    def validate_undefined_nodes_in_edges(cls, values):
        if 'nodes' not in values.keys() or 'edges' not in values.keys():
            return values       # Nodes did not pass upstream validations
        node_names = values['nodes'].keys()
        for edge_name, edge in values['edges'].items():
            if edge.source not in node_names:
                raise ValueError(f'Source for edge "{edge_name}" is not defined: "{edge.source}"')
            if edge.destination not in node_names:
                raise ValueError(f'Destination for edge "{edge_name}" is not defined: "{edge.destination}"')
        return values

    @property
    def structure(self) -> Dict[str, List[str]]:
        """For every node all the nodes it's connected to"""
        structure = {}
        for _, edge in self.edges.items():
            if edge.source not in structure.keys():
                structure[edge.source] = [edge.destination]
            else:
                structure[edge.source].append(edge.destination)

        return structure

    @property
    def non_source_nodes(self) -> List[str]:
        """All nodes that are the destination of an edge"""
        return [node for x in self.structure.values() for node in x]

    @property
    def sources(self) -> List[str]:
        """Nodes that are sources of the DAG"""
        sources = set(self.structure.keys())
        destinations = set(self.non_source_nodes)
        return list(sources.difference(destinations))

    def get_edge(self, source: str, destination: str) -> Edge:
        for edge_name, edge in self.edges.items():
            if edge.source == source and edge.destination == destination:
                return edge
        assert False

    def get_edge_name(self, source: str, destination: str) -> str:
        for edge_name, edge in self.edges.items():
            if edge.source == source and edge.destination == destination:
                return edge_name
        assert False

    def get_edges_for_source(self, source: str) -> List[str]:
        return [edge_name for edge_name, edge in self.edges.items() if edge.source == source]

    def out_nodes(self, source: str) -> List[str]:
        return self.structure.get(source, ())

    @property
    def has_cycle(self):
        white, gray, black = 0, 1, 2
        color = {n: white for n in self.nodes.keys()}

        def dfs(node):
            color[node] = gray
            for dest in self.structure.get(node, []):
                if color[dest] == gray:
                    return True
                elif color[dest] == white and dfs(dest):
                    return True
            color[node] = black
            return False

        for n in self.nodes:
            if color[n] == white and dfs(n):
                return True
        return False


class DagContext(BaseModel):
    execution_date: datetime
    etl_timestamp: datetime = Field(default=datetime.now())

    @property
    def ds(self) -> str:
        return self.execution_date.strftime('%Y-%m-%d')

    @property
    def ds_nodash(self) -> str:
        return self.execution_date.strftime('%Y%m%d')

    @property
    def ts(self) -> str:
        return self.execution_date.strftime('%Y-%m-%dT%H:%M:%S')


def hash_dag_code(dag_code: str) -> str:
    m = hashlib.sha1()
    m.update(dag_code.encode())
    return m.hexdigest()


class DagDeployment(BaseModel):
    dag_name: str
    deployment_date: datetime
    dag_code: str

    @property
    def deployment_hash(self) -> str:
        return hash_dag_code(self.dag_code)


class NodeDefinitionV2(BaseModel):
    input: Union[str, List[str], None] = Field(
        default=None,
        description='Task or tasks that will send their output as input to the current node'
    )
    function: str = Field(
        ...,
        regex=r'(typhoon\.\w+\.\w+|functions\.\w+\.\w+)',
        description="""Python function that will get called when the task runs.
                    If it is a built-in typhoon function it will have the following structure:
                      typhoon.[MODULE_NAME].[FUNCTION_NAME]
                    Whereas if it is a user defined function it will have the following structure:
                      functions.[MODULE_NAME].[FUNCTION_NAME]"""
    )
    asynchronous: bool = Field(
        default=True,
        description="""If set to TRUE it will run the function in a different lambda instance.
                    This is useful when you want to increase parallelism. There is currently no framework cap on
                    parallelism though so if that is an issue set it to FALSE so it will run the batches one by one."""
    )
    args: Dict[str, Any] = Field(default={})

    @validator('args')
    def validate_args_keys(cls, v):
        for k in v.keys():
            if not re.match(r'\w+(\s*=>\s*APPLY)?', k):
                raise ValueError(
                    f'Config key "{k}" does not match pattern. Must be identifier with optional => APPLY')
        return v

    def make_config(self) -> dict:
        result = {}
        for k, v in self.args.items():
            if not is_apply(k) or not uses_batch(v):
                result[k] = v
        return result

    def make_adapter(self) -> dict:
        result = {}
        for k, v in self.args.items():
            if is_apply(k) and uses_batch(v):
                result[k] = v
        return result


class DAGDefinitionV2(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your DAG')
    schedule_interval: str = Field(
        ...,
        regex='(' + '@hourly|@daily|@weekly|@monthly|@yearly|' +
              r'((\*|\?|\d+((\/|\-){0,1}(\d+))*)\s*){5,6}' + '|' +
              r'rate\(\s*1\s+minute\s*\)' + '|' +
              r'rate\(\s*\d+\s+minutes\s*\)' + '|' +
              r'rate\(\s1*\d+\s+hour\s*\)' + '|' +
              r'rate\(\s*\d+\s+hours\s*\)' + '|' +
              ')',
        description='Schedule or frequency on which the DAG should run'
    )
    active: bool = Field(True, description='Whether to deploy the DAG or not')
    tasks: Dict[str, NodeDefinitionV2]
    # tests: Optional[Test]

    def make_dag(self) -> DAG:
        nodes = {
            task_name: Node(
                function=task.function,
                asynchronous=task.asynchronous,
                config=task.make_config()
            )
            for task_name, task in self.tasks.items()
        }
        edges = {}
        edge_id = 1
        for task_name, task in self.tasks.items():
            if task.input:
                inp = task.input if isinstance(task.input, list) else [task.input]
                for source_task_id in inp:
                    edges[f'e{edge_id}'] = Edge(
                        source=source_task_id,
                        destination=task_name,
                        adapter=task.make_adapter(),
                    )
                    edge_id += 1

        return DAG(
            name=self.name,
            schedule_interval=self.schedule_interval,
            active=self.active,
            nodes=nodes,
            edges=edges,
        )


def is_apply(k):
    return k.endswith(' => APPLY')


def uses_batch(item):
    if isinstance(item, str):
        return '$BATCH' in item
    elif isinstance(item, list):
        return any(uses_batch(x) for x in item)
    elif isinstance(item, dict):
        return any(uses_batch(v) for k, v in item.items())
    assert False


if __name__ == '__main__':
    import yaml
    dag_v2 = """
name: example_v2
schedule_interval: "@hourly"

tasks:
    tables:
        function: typhoon.flow_control.branch
        args:
            branches:
                - sheep
                - dog
    
    extract:
        input: tables
        function: typhoon.relational.query
        asynchronous: False
        args:
            sql => APPLY: f'select * from {$BATCH}'
            hook => APPLY: $HOOKS.oracle_db
            batch_size: 500
    
    load:
        input: extract
        function: typhoon.filesystem.write_data
        args:
            hook => APPLY: $HOOKS.data_lake
            data => APPLY: typhoon.transformations.write_csv($BATCH.data)
            path => APPLY: f'{$BATCH.table_name}/part{$BATCH_NUM}.csv'
    """
    print(yaml.safe_dump(DAGDefinitionV2.parse_obj(yaml.safe_load(dag_v2)).make_dag().dict(), sort_keys=False))
