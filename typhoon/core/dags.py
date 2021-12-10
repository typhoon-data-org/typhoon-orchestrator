import hashlib
import importlib
import itertools
import os
import re
import sys
from datetime import datetime, timedelta
from enum import Enum
from types import SimpleNamespace
from typing import List, Union, Dict, Any, Optional, Tuple

import yaml
from croniter import croniter
from dataclasses import dataclass
from dateutil.parser import parse
from pydantic import BaseModel, validator, Field, root_validator

from typhoon.core.cron_utils import aws_schedule_to_cron
from typhoon.core.settings import Settings
from typhoon.introspection.introspect_extensions import get_typhoon_extensions_info

IDENTIFIER_REGEX = r'\w+'
Identifier = Field(..., regex=r'\w+')


@dataclass
class Py:
    value: str
    key: Optional[str] = None
    args_dependencies: Optional[List[str]] = None

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
        code = re.sub(r'typhoon\.(\w+)\.(\w+)', r'typhoon_transformations_\g<1>.\g<2>', code)
        code = code.replace('$ARG', 'component_args')
        return code

    @staticmethod
    def construct(loader: yaml.Loader, node: yaml.Node):
        return construct_custom_class(Py, loader, node)

    @staticmethod
    def represent(dumper: yaml.Dumper, data: 'Py'):
        return dumper.represent_scalar('!Py', data.value)

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
    config_name: str = 'args'

    @staticmethod
    def construct(loader: yaml.Loader, node: yaml.Node):
        return construct_custom_class(MultiStep, loader, node)

    @staticmethod
    def represent(dumper: yaml.Dumper, data: 'MultiStep'):
        return dumper.represent_sequence('MultiStep', data.value)

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
            if isinstance(x, str):
                x = f'"""{x}"""'
            steps.append(f'{self.key}_{i + 1} = {x}')
        return '\n'.join(steps) + '\n' + f"{self.config_name}['{self.key}'] = {self.key}_{len(steps)}"

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


class Granularity(str, Enum):
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    HOUR = 'hour'
    MINUTE = 'minute'
    SECOND = 'second'


class DagContext(BaseModel):
    interval_start: datetime = Field(
        ..., description='Date representing the start of the interval for which we want to get data')
    interval_end: datetime = Field(
        ..., description='Date representing the end of the interval for which we want to get data')
    execution_time: datetime = Field(default_factory=lambda: datetime.now(), description='Date')
    granularity: Granularity = Field(default='daily', description='Granularity of DAG')

    def __init__(self, **data):
        super().__init__(**data)
        granularity = self.granularity
        if granularity == 'minute':
            self.interval_start = self.interval_start.replace(second=0, microsecond=0)
            self.interval_end = self.interval_end.replace(second=0, microsecond=0)
        elif granularity == 'hour':
            self.interval_start = self.interval_start.replace(minute=0, second=0, microsecond=0)
            self.interval_end = self.interval_end.replace(minute=0, second=0, microsecond=0)
        elif granularity == 'day':
            self.interval_start = self.interval_start.replace(hour=0, minute=0, second=0, microsecond=0)
            self.interval_end = self.interval_end.replace(hour=0, minute=0, second=0, microsecond=0)
        elif granularity == 'month':
            self.interval_start = self.interval_start.replace(day=0, hour=0, minute=0, second=0, microsecond=0)
            self.interval_end = self.interval_end.replace(day=0, hour=0, minute=0, second=0, microsecond=0)
        elif granularity == 'year':
            self.interval_start = self.interval_start.replace(month=0, day=0, hour=0, minute=0, second=0, microsecond=0)
            self.interval_end = self.interval_end.replace(month=0, day=0, hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def from_cron_and_event_time(
            schedule_interval: str,
            event_time: Union[datetime, str],
            granularity: str,
    ) -> 'DagContext':
        if not isinstance(event_time, datetime):
            event_time = parse(event_time)
        cron = aws_schedule_to_cron(schedule_interval)
        iterator = croniter(cron, event_time + timedelta(seconds=1))   # In case the event is exactly on time
        interval_end = iterator.get_prev(datetime)
        interval_start = iterator.get_prev(datetime)
        return DagContext(interval_start=interval_start, interval_end=interval_end, granularity=granularity)


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
    interval_start: datetime = Field(
        default=None,
        description='Date representing the start of the interval for which we want to get data',
    )
    interval_end: datetime = Field(
        default=None,
        description='Date representing the end of the interval for which we want to get data',
    )

    @property
    def dag_context(self) -> DagContext:
        interval_start = self.interval_start or datetime.now()
        return DagContext(
            interval_start=interval_start,
            interval_end=self.interval_end or (interval_start - timedelta(days=1))
        )

    @property
    def custom_locals(self) -> dict:
        result = {}
        try:
            import pandas as pd
            result['pd'] = pd
        except ImportError:
            print('Warning: could not import pandas. Run pip install pandas if you want to use dataframes')
        result['SimpleNamespace'] = SimpleNamespace

        # Import functions
        from typhoon.core.transpiler.transpiler_helpers import extract_imports, typhoon_import_transformation_as

        extensions_info = get_typhoon_extensions_info()
        for k, v in extensions_info['transformations'].items():
            import_from = importlib.import_module(v)
            import_as = typhoon_import_transformation_as(k)
            result[import_as] = import_from
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


def construct_template(loader: yaml.Loader, node: yaml.Node):
    template_str = loader.construct_yaml_str(node)
    return Py(f"jinja2.Template('{template_str}')")


def add_yaml_constructors():
    yaml.add_constructor('!Py', Py.construct)
    yaml.add_constructor('!Hook', construct_hook)
    yaml.add_constructor('!Var', construct_variable)
    yaml.add_constructor('!MultiStep', MultiStep.construct)
    yaml.add_constructor('!PyObj', construct_python_object)
    yaml.add_constructor('!DataFrame', construct_dataframe)
    yaml.add_constructor('!Template', construct_template)


def represent_granularity(dumper: yaml.Dumper, data: Granularity):
    return dumper.represent_str(data.value)


def add_yaml_representers(dumper: yaml.Dumper):
    yaml.add_representer(Py, Py.represent, dumper)
    yaml.add_representer(MultiStep, MultiStep.represent, dumper)
    yaml.add_representer(Granularity, represent_granularity, dumper)


def get_deps_uses_batch_and_warnings(item):
    deps = []
    warnings = []

    def _uses_batch(item):
        if isinstance(item, Py):
            nonlocal deps
            if item.args_dependencies:
                deps += item.args_dependencies
            return '$BATCH' in item.value
        if isinstance(item, MultiStep):
            return _uses_batch(item.value)
        elif isinstance(item, list):
            items_use_batch = [_uses_batch(x) for x in item]
            return any(items_use_batch)
        elif isinstance(item, dict):
            items_use_batch = [_uses_batch(v) for k, v in item.items()]
            return any(items_use_batch)
        elif isinstance(item, str):
            for x in ['$BATCH', '$DAG_CONTEXT', '$VARIABLE', '$HOOK', '$ARG']:
                if x in item:
                    warnings.append('WARNING: Argument {arg} is a string and contains' + f' "{x}". Did you mean to make it !Py?')
            return False
        elif isinstance(item, (float, int)):
            return False
        assert False, f'Found type {type(item)} with value {item}'
    arg_uses_batch = _uses_batch(item)
    return deps, arg_uses_batch, warnings


class TaskDefinition(BaseModel):
    input: Union[str, List[str], None] = Field(
        default=None,
        description='Task or tasks that will send their output as input to the current node'
    )
    component: Optional[str] = Field(
        default=None,
        regex=r'(typhoon\.\w+|components\.\w+)',
        description="""Typhoon component that will get substituted for its tasks.
                    If it is a built-in typhoon component it will have the following structure:
                      typhoon.[COMPONENT_NAME]
                    Whereas if it is a user defined component it will have the following structure:
                      components.[COMPONENT_NAME]"""
    )
    function: Optional[str] = Field(
        default=None,
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
            # HACK: We don't have custom JSON decoders so we need to create Py and MultiStep objects here if applicable
            if isinstance(v, dict) and v.get('__typhoon__') == 'Py':
                val[k] = Py(v['value'])
            elif isinstance(v, dict) and v.get('__typhoon__') == 'MultiStep':
                val[k] = MultiStep(v['value'])
        return val

    @validator('function')
    def validate_function(cls, val, values, **kwargs):
        if val is not None and 'component' in values.keys() and values['component'] is not None:
            raise ValueError('Function and component are mutually exclusive')
        elif val is None and ('component' not in values.keys() or values['component'] is None):
            raise ValueError('Either function or component is necessary')
        return val

    # def make_config(self) -> dict:
    #     result = {}
    #     for k, v in self.args.items():
    #         if not uses_batch(v):
    #             result[k] = v
    #     return result

    # def make_adapter(self) -> dict:
    #     result = {}
    #     for k, v in self.args.items():
    #         if uses_batch(v):
    #             result[k] = v
    #     return result

    def make_adapter_and_config(self) -> Tuple[dict, dict]:
        arg_deps = {}
        args_that_use_batch = []
        for k, v in self.args.items():
            deps, arg_uses_batch, warnings = get_deps_uses_batch_and_warnings(v)
            for warning in warnings:
                print(warning.format(arg=k))
            arg_deps[k] = deps
            if arg_uses_batch:
                args_that_use_batch.append(k)

        args_that_indirectly_use_batch = [
            x
            for x in self.args.keys() if not x.startswith('_') and any(d in args_that_use_batch for d in arg_deps[x])
        ]
        args_that_use_batch += args_that_indirectly_use_batch
        component_args_needed_by_args_that_use_batch = list(
            itertools.chain(*[arg_deps[x] for x in args_that_use_batch]))
        adapter = {}
        for arg in [*component_args_needed_by_args_that_use_batch, *args_that_use_batch]:
            adapter[arg] = self.args[arg]

        config = {}
        component_args_that_dont_use_batch = [
            x for x in self.args.keys() if not x.startswith('_') and x not in args_that_use_batch
        ]
        component_args_needed_by_args_that_dont_use_batch = list(
            itertools.chain(*[arg_deps[x] for x in component_args_that_dont_use_batch]))
        for arg in [*component_args_needed_by_args_that_dont_use_batch, *component_args_that_dont_use_batch]:
            config[arg] = self.args[arg]
        return adapter, config

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
        adapter, _ = self.make_adapter_and_config()
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

        # Import functions
        from typhoon.core.transpiler.transpiler_helpers import extract_imports, typhoon_import_transformation_as

        extensions_info = get_typhoon_extensions_info()
        for k, v in extensions_info['transformations'].items():
            import_from = importlib.import_module(v)
            import_as = typhoon_import_transformation_as(k)
            custom_locals[import_as] = import_from

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
    description: Optional[str] = Field(None, description='Description of what the DAG does for documentation purposes')
    schedule_interval: str = Field(
        ...,
        regex='(' + '@hourly|@daily|@weekly|@monthly|@yearly|' +
              r'((\*|\?|\d+((\/|\-){0,1}(\d+))*)\s*){5,6}' + '|' +
              r'rate\(\s*1\s+minute\s*\)' + '|' +
              r'rate\(\s*\d+\s+minutes\s*\)' + '|' +
              r'rate\(\s*1\s+hour\s*\)' + '|' +
              r'rate\(\s*\d+\s+hours\s*\)' + '|' +
              r'rate\(\s*1\s+day\s*\)' + '|' +
              r'rate\(\s*\d+\s+days\s*\)' +
              ')',
        description='Schedule or frequency on which the DAG should run'
    )
    granularity: Optional[Granularity] = Field(
        default=None,
        description='Granularity of DAG. If not specified it will guess based on the expected time between runs.',
    )
    active: bool = Field(True, description='Whether to deploy the DAG or not')
    tasks: Dict[str, TaskDefinition]
    tests: Optional[Dict[str, TestCase]]
    airflow_default_args: Optional[dict] = Field(
        None, description='Arguments passed to generated Airflow DAG. Ignored if deploy target is not airflow.')

    def guess_granularity(self) -> Granularity:
        cron = aws_schedule_to_cron(self.schedule_interval)
        iterator = croniter(cron)
        interval = iterator.get_next(datetime)
        next_interval = iterator.get_next(datetime)
        delta = (next_interval - interval)
        if delta < timedelta(minutes=1):
            return Granularity.SECOND
        elif delta < timedelta(hours=1):
            return Granularity.MINUTE
        elif delta < timedelta(days=1):
            return Granularity.HOUR
        elif delta < timedelta(days=31):
            return Granularity.DAY
        elif delta < timedelta(days=365):
            return Granularity.MONTH
        else:
            return Granularity.YEAR

    @property
    def sources(self) -> Dict[str, TaskDefinition]:
        return {k: v for k, v in self.tasks.items() if v.input is None}

    def assert_tests(self, task_name, batch, batch_num, dag_context):
        task = self.tasks[task_name]
        test_case = self.tests[task_name]
        execute_results = task.execute_adapter(batch, dag_context, batch_num)
        for k, v in test_case.expected.items():
            assert v == execute_results[k]

    def substitute_components(self):
        from typhoon.core.glue import load_components
        from typhoon.core import components

        typhoon_components = {
            c.name: c
            for c, _ in load_components(ignore_errors=False, kind='typhoon')
        }
        custom_components = {
            c.name: c
            for c, _ in load_components(ignore_errors=False, kind='custom')
        }
        tasks_to_add = {}
        tasks_to_remove = []
        for task_name, task in self.tasks.items():
            if task.input is not None and '.' in task.input:
                input_task, component_output = task.input.split('.')
                component_name = self.tasks[input_task].component
                if component_name.startswith('typhoon.'):
                    component = typhoon_components.get(component_name.split('.')[1])
                else:
                    component = custom_components.get(component_name.split('.')[1])
                if not component.can_connect(component_output):
                    raise ValueError(f'Can not connect {component_name} in task {task_name}. Does not have output {component_output}')
                task.input = components.task_name(name_in_dag=input_task, task=component_output)
            if task.component is not None:
                component_name = task.component
                if component_name.startswith('typhoon.'):
                    component = typhoon_components.get(component_name.split('.')[1])
                else:
                    component = custom_components.get(component_name.split('.')[1])
                if component is None:
                    raise ValueError(f'No component found for {component_name}')
                component_tasks = component.make_tasks(name_in_dag=task_name, input_task=task.input, input_arg_values=task.args)
                tasks_to_add.update(**component_tasks)
                tasks_to_remove.append(task_name)
        for task_name in tasks_to_remove:
            del self.tasks[task_name]
        self.tasks.update(**tasks_to_add)

    class Config:
        json_encoders = {
            Py: lambda v: {'__typhoon__': 'Py', 'value': v.value},
            MultiStep: lambda v: {'__typhoon__': 'MultiStep', 'value': v.value},
        }


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
    add_yaml_representers(yaml.SafeDumper)
    print(yaml.safe_dump({
        'a': 'abc',
        'b': [1, 2, 3],
        'c': Py('abcd'),
        'd': MultiStep([1, 'a', 2]),
    }))
