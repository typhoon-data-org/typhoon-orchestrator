import hashlib
import os
import re
import sys
from datetime import datetime, date
from enum import Enum
from types import SimpleNamespace
from typing import List, Union, Dict, Any, Optional

import yaml
from dataclasses import dataclass
from pydantic import BaseModel, validator, Field, root_validator
from typhoon.core.settings import Settings

IDENTIFIER_REGEX = r'\w+'
Identifier = Field(..., regex=r'\w+')


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
        code = re.sub(r'typhoon\.(\w+\.\w+)', r'typhoon_transformations.\g<1>', code)
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
        def add_key(item):
            if isinstance(item, Py):
                item.key = self.key
            elif isinstance(item, list):
                return [add_key(x) for x in item]
            elif isinstance(item, dict):
                return {k: add_key(v) for k, v in item.items()}
            else:
                return item
        steps = []
        for i, x in enumerate(self.value):
            add_key(x)
            steps.append(f'{self.key}_{i + 1} = {x}')
        return '\n'.join(steps) + '\n' + f"config['{self.key}'] = {self.key}_{len(steps)}"

    def __str__(self):
        if not isinstance(self.value, list) or self.key is None:
            return f'MultiStep({self.value.__repr__()})'
        else:
            return self.transpile()

    def __repr__(self):
        return str(self)


Item = Union[int, str, float, Dict, List, Py, MultiStep]


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


class TestCase(BaseModel):
    batch: Any = Field(..., description='Sample batch')
    expected: Dict[str, Any] = Field(..., description='Expected result')
    batch_num: int = Field(
        default=1,
        description='Batch number for the test. If more than one is provided it will run the tests for each')
    execution_date: Union[datetime, None] = Field(default=None, description='Execution date')

    @property
    def dag_context(self) -> DagContext:
        return DagContext(execution_date=self.execution_date or datetime.now())

    @property
    def custom_locals(self) -> dict:
        result = {}
        try:
            import pandas as pd
            result['pd'] = pd
        except ImportError:
            print('Warning: could not import pandas. Run pip install pandas if you want to use dataframes')
        return result

    @property
    def evaluated_batch(self):
        return evaluate_item(self.custom_locals, self.batch)

    @property
    def evaluated_expected(self):
        result = {}
        for k, v in self.expected.items():
            result[k] = evaluate_item(self.custom_locals, v)
        return result


def construct_python_object(loader: yaml.Loader, node: yaml.Node) -> SimpleNamespace:
    result = SimpleNamespace.__new__(SimpleNamespace)
    yield result
    attributes = loader.construct_mapping(node)
    if not isinstance(attributes, dict):
        raise ValueError(f'Error constructing PyObj. Expected dictionary, found {attributes}')
    result.__init__(**attributes)


@dataclass
class DataFrameFuture:
    data: Union[List[dict], Dict[str, list]]


def construct_dataframe(loader: yaml.Loader, node: yaml.Node):
    result = DataFrameFuture.__new__(DataFrameFuture)
    yield result
    if isinstance(node, yaml.SequenceNode):
        data = loader.construct_sequence(node)
    elif isinstance(node, yaml.MappingNode):
        data = loader.construct_mapping(node)
    else:
        raise ValueError(f'Error constructing DataFrame. Expected dictionary or array of dictionaries, found {type(node)}')
    result.__init__(data)


def add_yaml_constructors():
    yaml.add_constructor('!Py', Py.construct)
    yaml.add_constructor('!Hook', construct_hook)
    yaml.add_constructor('!Var', construct_variable)
    yaml.add_constructor('!MultiStep', MultiStep.construct)
    yaml.add_constructor('!PyObj', construct_python_object)
    yaml.add_constructor('!DataFrame', construct_dataframe)


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
            if not re.fullmatch(IDENTIFIER_REGEX, k):
                raise ValueError(f'Arg "{k}" should be an identifier')
            if isinstance(v, MultiStep):
                v.key = k
        return val

    def make_config(self) -> dict:
        result = {}
        for k, v in self.args.items():
            if not uses_batch(v):
                result[k] = v
        return result

    def make_adapter(self) -> dict:
        result = {}
        for k, v in self.args.items():
            if uses_batch(v):
                result[k] = v
        return result

    @staticmethod
    def load_custom_transformations_namespace() -> object:
        custom_transformation_modules = {}
        transformations_path = str(Settings.transformations_directory)
        for filename in os.listdir(transformations_path):
            if filename == '__init__.py' or not filename.endswith('.py'):
                continue
            module_name = filename[:-3]
            module = load_module_from_path(
                module_path=os.path.join(transformations_path, filename),
                module_name=module_name,
            )
            custom_transformation_modules[module_name] = module
        custom_transformations = SimpleNamespace(**custom_transformation_modules)
        return custom_transformations

    # noinspection PyUnresolvedReferences
    def execute_adapter(self, batch: Any, dag_context: DagContext, batch_num=1, no_custom_transformations=False)\
            -> Dict[str, Any]:
        adapter = self.make_adapter()
        if not no_custom_transformations:
            custom_transformations_ns = self.load_custom_transformations_namespace()
        else:
            custom_transformations_ns = None

        import typhoon.contrib.transformations as typhoon_transformations
        custom_locals = locals()
        custom_locals['Settings'] = Settings
        custom_locals['transformations'] = custom_transformations_ns
        custom_locals['typhoon_transformations'] = typhoon_transformations
        custom_locals['batch'] = batch
        custom_locals['batch_num'] = batch_num
        custom_locals['dag_context'] = dag_context

        os.environ['TYPHOON_ENV'] = 'dev'

        results = {}
        for k, v in adapter.items():
            results[k] = evaluate_item(custom_locals, v)

        return results


class BrokenImportError(object):
    pass


def load_module_from_path(module_path, module_name=None, must_exist=True):
    import sys
    from importlib import util

    sys.path.append(os.path.dirname(os.path.dirname(module_path)))
    if module_name is None:
        parts = module_path.split('/')
        module_name = parts[-2] + '.' + parts[-1].strip('.py')
    spec = util.spec_from_file_location(module_name, module_path)
    module = util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (NameError, SyntaxError, FileNotFoundError):
        if must_exist:
            raise BrokenImportError
        else:
            print(f'Module {module_name} at path {module_path} does not exist')
            return None
    return module


@dataclass
class ArgEvaluationError:
    code: str
    error_type: Exception
    error_value: Exception
    traceback: object


def evaluate_item(custom_locals, item) -> Any:
    if isinstance(item, Py):
        code = item.transpile()
        try:
            result = eval(code, {}, custom_locals)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            result = ArgEvaluationError(code, exc_type, exc_value, exc_traceback)
            print(code)
            raise e
        return result
    elif isinstance(item, MultiStep):
        custom_locals_copy = custom_locals.copy()
        sentinel = object()
        result = None
        for code_line in item.transpile().split('\n'):
            left, right = code_line.split('=')
            name = left.strip()
            code = right.strip()
            try:
                result = eval(code, {}, custom_locals_copy)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                result = ArgEvaluationError(code, exc_type, exc_value, exc_traceback)
            if 'config[' in name:
                return result
            else:
                custom_locals_copy[name] = result
        assert result is not sentinel
        return result
    elif isinstance(item, list):
        return [evaluate_item(custom_locals, x) for x in item]
    elif isinstance(item, dict):
        return {k: evaluate_item(custom_locals, v) for k, v in item.items()}
    elif isinstance(item, SimpleNamespace):
        for k, v in item.__dict__.items():
            item.__setattr__(k, evaluate_item(custom_locals, v))
        return item
    elif isinstance(item, DataFrameFuture):
        import pandas as pd
        return pd.DataFrame(item.data)
    else:
        return item


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
    tests: Optional[Dict[str, TestCase]]

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

    def assert_tests(self, task_name, batch, batch_num, dag_context):
        task = self.tasks[task_name]
        test_case = self.tests[task_name]
        execute_results = task.execute_adapter(batch, dag_context, batch_num)
        for k, v in test_case.expected.items():
            assert v == execute_results[k]


def uses_batch(item):
    if isinstance(item, Py):
        return '$BATCH' in item.value
    if isinstance(item, MultiStep):
        return uses_batch(item.value)
    elif isinstance(item, list):
        return any(uses_batch(x) for x in item)
    elif isinstance(item, dict):
        return any(uses_batch(v) for k, v in item.items())
    elif isinstance(item, (str, float, int)):
        return False
    assert False, f'Found type {type(item)} with value {item}'


if __name__ == '__main__':
    add_yaml_constructors()
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
            sql: !Py f'select * from {$BATCH}'
            hook: !Py $HOOKS.oracle_db
            batch_size: 500
    
    load:
        input: extract
        function: typhoon.filesystem.write_data
        args:
            hook: !Py $HOOKS.data_lake
            data: !Py typhoon.transformations.write_csv($BATCH.data)
            path: !Py f'{$BATCH.table_name}/part{$BATCH_NUM}.csv'
    """
    print(yaml.dump(DAGDefinitionV2.parse_obj(yaml.load(dag_v2, yaml.FullLoader)).make_dag().dict(), Dumper=yaml.Dumper, sort_keys=False))
