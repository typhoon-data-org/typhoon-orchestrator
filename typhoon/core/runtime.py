from types import SimpleNamespace, GeneratorType
from typing import Callable, Any, List, Optional, Union, Type, Dict
from uuid import uuid4

from typhoon.core import DagContext, SKIP_BATCH
from typing_extensions import Protocol, runtime_checkable


class BrokerInterface(Protocol):
    def send(
            self,
            dag_context: DagContext,
            source_id: str,
            destination: 'TaskInterface',
            batch_num: int,
            batch: Any,
            batch_group_id: str,    # Since a batch_number is not enough to guarantee uniqueness
    ):
        ...


class DebugBroker(BrokerInterface):
    batches: Dict[str, Dict[int, Any]]

    def __init__(self):
        self.batches = {}

    def send(
            self,
            dag_context: DagContext,
            source_id: str,
            destination: 'TaskInterface',
            batch_num: int,
            batch: Any,
            batch_group_id: str,
    ):
        if batch_group_id not in self.batches.keys():
            self.batches[batch_group_id] = {}
        self.batches[batch_group_id][batch_num] = batch
        print(f'# Batch group {batch_group_id}\n# Batch number {batch_num}\n{batch}\n')


class SequentialBroker(BrokerInterface):
    def send(
            self,
            dag_context: DagContext,
            source_id: str,
            destination: 'TaskInterface',
            batch_num: int,
            batch: Any,
            batch_group_id: str,
    ):
        destination.run(dag_context, source_id, batch_num, batch)


@runtime_checkable
class TaskInterface(Protocol):
    task_id: str
    function: Callable
    broker: BrokerInterface
    destinations: List['TaskInterface']
    parent_component: Optional['ComponentInterface']

    def set_destination(self, task: Union['TaskInterface', 'ComponentInterface']):
        self.destinations.append(task)

    def get_args(self, dag_context: DagContext, source: str, batch_num: int, batch: Any) -> dict:
        ...

    def run(self, dag_context: DagContext, source: Optional[str],  batch_num: int, batch: Any):
        args = self.get_args(dag_context, source, batch_num, batch)
        batch_group_id = str(uuid4())
        batches = self.function(**args)
        if not isinstance(batches, GeneratorType):
            batches = [batches]
        for batch_num, batch in enumerate(batches, start=1):
            if batch is SKIP_BATCH:
                print(f'Skipping batch {batch_num} for {self.task_id}')
                continue
            for destination in self.destinations:
                self.broker.send(dag_context, self.task_id, destination, batch_num, batch, batch_group_id)

    def copy(
            self,
            task_id: str = None,
            function: Callable = None,
            broker: BrokerInterface = None,
            destinations: List['TaskInterface'] = None,
            parent_component: Optional['ComponentInterface'] = None,
    ) -> 'TaskInterface':
        new_task = self.__class__.__new__(self.__class__)
        new_task.task_id = task_id if task_id is not None else self.task_id
        new_task.function = function if function is not None else self.function
        new_task.broker = broker if broker is not None else self.broker
        new_task.destinations = destinations if destinations is not None else self.destinations
        new_task.parent_component = parent_component if parent_component is not None else self.parent_component
        return new_task


class ComponentArgs(Protocol):
    dag_context: DagContext
    source: str
    batch_num: int
    batch: Any
    parent_component: Optional['ComponentInterface']
    _args_cache: Optional[dict]

    def __init__(self, dag_context: DagContext, source: str, batch_num: int, batch: Any):
        self.dag_context = dag_context
        self.source = source
        self.batch = batch
        self.batch_num = batch_num

    def get_args(self) -> dict:
        ...

    def __getattr__(self, item):
        if self._args_cache is None:
            self._args_cache = self.get_args()
        return self._args_cache[item]


@runtime_checkable
class ComponentInterface(Protocol):
    task_id: str
    dag_context: DagContext
    args_class: Type[ComponentArgs]
    component_sources: List[TaskInterface]
    output: SimpleNamespace

    def get_batch_dependent_args(self, source: str, batch_num: int, batch: Any) -> dict:
        ...

    def run(self, dag_context: DagContext, source: Optional[str],  batch_num: int, batch: Any):
        for component_source in self.component_sources:
            component_source.run(dag_context, source, batch_num, batch)
