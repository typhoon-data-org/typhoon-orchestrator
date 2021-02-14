import hashlib
import re
from datetime import datetime
from enum import Enum
from typing import List, Union, Dict, Any, Optional

import yaml
from dataclasses import dataclass
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


# TestResult = Union[True, str, Exception]


# class TestCase(BaseModel):
#     batch: Any = Field(..., description='Sample batch')
#     expected: Dict[str, Any] = Field(..., description='Expected result')
#
#     def assert_test(self, args, input_data) -> List[TestResult]:
#         for k, v in self.expected.items():
#             run_transformations(args, input_data)


@dataclass
class Py:
    value: str
    key: Optional[str] = None

    def transpile(self) -> str:
        code = self.value
        code = code.replace('$BATCH_NUM', 'batch_num')
        code = code.replace('$BATCH', 'batch')
        code = re.sub(r'\$DAG_CONTEXT(\.(\w+))', r'dag_context.\g<2>', code)
        code = code.replace('$DAG_CONTEXT', 'dag_context')
        if self.key is not None:
            code = re.sub(r'\$(\d)+', r"{key}_\g<1>".format(key=self.key), code)
        code = re.sub(r'\$HOOK(\.(\w+))', r'get_hook("\g<2>")', code)
        code = re.sub(r'\$VARIABLE(\.(\w+))', r'Settings.metadata_store().get_variable("\g<2>").get_contents()', code)
        return code

    @staticmethod
    def construct(loader: yaml.Loader, node: yaml.Node):
        return construct_custom_class(Py, loader, node)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, Py):
            raise TypeError(f'Expected Py object, found {v}')
        if not isinstance(v.value, str):
            raise TypeError(f'string required, found {v.value}')
        return v

    def __str__(self):
        """If a key is set, unquoted string. Otherwise print Py(...)"""
        return self.transpile()

    def __repr__(self):
        return str(self)


def construct_custom_class(cls, loader: yaml.Loader, node: yaml.Node):
    result = cls.__new__(cls)
    yield result
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node)
    else:
        assert False
    result.__init__(value)


def construct_hook(loader: yaml.Loader, node: yaml.Node) -> Py:
    conn_name = loader.construct_yaml_str(node)
    if not re.match(r'\w+', conn_name):
        raise ValueError(f'Error constructing hook. Expected connection name, found {conn_name}')
    return Py(f'$HOOK.{conn_name}')


def construct_variable(loader: yaml.Loader, node: yaml.Node) -> Py:
    var_id = loader.construct_yaml_str(node)
    if not re.match(r'\w+', var_id):
        raise ValueError(f'Error constructing variable. Expected variable id, found {var_id}')
    return Py(f'$VARIABLE.{var_id}')


@dataclass
class MultiStep:
    value: list
    key: Optional[str] = None

    @staticmethod
    def construct(loader: yaml.Loader, node: yaml.Node):
        return construct_custom_class(MultiStep, loader, node)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, MultiStep):
            raise TypeError(f'Expected MultiStep object, found {v}')
        if not isinstance(v.value, list):
            raise TypeError(f'list required, found {v.value}')
        return v

    def transpile(self) -> str:
        steps = []
        for i, x in enumerate(self.value):
            if isinstance(x, Py):
                x.key = self.key
            steps.append(f'{self.key}_{i + 1} = {x}')
        return '\n'.join(steps) + '\n' + f"config['{self.key}'] = {self.key}_{len(steps)}"

    def __str__(self):
        if not isinstance(self.value, list) or self.key is None:
            return f'MultiStep({self.value.__repr__()})'
        else:
            return self.transpile()

    def __repr__(self):
        return str(self)


def add_yaml_constructors():
    yaml.add_constructor('!Py', Py.construct)
    yaml.add_constructor('!Hook', construct_hook)
    yaml.add_constructor('!Var', construct_variable)
    yaml.add_constructor('!MultiStep', MultiStep.construct)


class TaskDefinition(BaseModel):
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
    def validate_args_keys(cls, val):
        # Decorate MultiStep with key name if necessary
        for k, v in val.items():
            if isinstance(v, MultiStep):
                v.key = k
        return val

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
    tasks: Dict[str, TaskDefinition]
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
