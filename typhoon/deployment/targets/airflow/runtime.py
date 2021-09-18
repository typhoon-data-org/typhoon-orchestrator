from datetime import datetime
from typing import Any, Union, Optional

from airflow import settings
from airflow.models import XCom, DAG
from airflow.operators.python_operator import PythonOperator
from dataclasses import dataclass

from typhoon.core import DagContext
from typhoon.core.runtime import BrokerInterface, TaskInterface, SequentialBroker, ComponentInterface


@dataclass
class AirflowBroker(BrokerInterface):
    dag_id: str

    def send(
            self,
            dag_context: DagContext,
            source_id: str,
            destination: 'TaskInterface',
            batch_num: int,
            batch: Any,
            batch_group_id: int,
    ):
        XCom.set(
            key=f'{batch_group_id}:{batch_num}',
            value=(batch_num, batch),
            execution_date=dag_context.execution_time,
            task_id=source_id,
            dag_id=self.dag_id,
        )


def make_typhoon_dag_context(context):
    interval_start = (context['dag_run'] and (context['dag_run'].conf or {}).get('interval_start')) or context['execution_date']
    interval_end = (context['dag_run'] and (context['dag_run'].conf or {}).get('interval_end')) or context['next_execution_date']
    execution_time = context['dag_run'].start_date if context['dag_run'] else context['ti'].execution_date
    dag_context = DagContext(interval_start=interval_start, interval_end=interval_end, execution_time=execution_time)
    return dag_context


def run_airflow_task(source: Optional[str], task: TaskInterface, **context):
    dag_context = make_typhoon_dag_context(context)
    if source is not None:
        session = settings.Session()
        batches = {
            x.key: x.value for x in
            session.query(XCom).filter(
                XCom.task_id == source,
                XCom.dag_id == context['dag'].dag_id,
                XCom.execution_date == dag_context.execution_time,
            ).order_by(XCom.key)
        }
        batch_groups = {}
        # They are sorted by XCom key so no need to worry about order
        for key, batch_and_batch_num in batches.items():
            batch_group_id = key.split(':')[0]
            if batch_group_id not in batch_groups.keys():
                batch_groups[batch_group_id] = [batch_and_batch_num]
            else:
                batch_groups[batch_group_id].append(batch_and_batch_num)
        for batch_group_id, batch_group in batch_groups.items():
            for batch_num, batch in batch_group:
                task.run(dag_context, source, batch_num, batch)
    else:
        task.run(dag_context, None, 1, None)


def make_airflow_task(source: str, task: TaskInterface, custom_task_id: str = None) -> PythonOperator:
    return PythonOperator(
        task_id=custom_task_id or task.task_id,
        python_callable=run_airflow_task,
        provide_context=True,
        op_kwargs={
            'source': source,
            'task': task,
        }
    )


def make_airflow_tasks(
        dag: DAG,
        task: Union[TaskInterface, ComponentInterface],
        source_airflow_task: PythonOperator = None,
        custom_source_id: str = None,
        airflow_version: int = 1,
):
    source_id = custom_source_id or (source_airflow_task.task_id if source_airflow_task is not None else None)
    if isinstance(task, ComponentInterface):
        for source_task in task.component_sources:
            make_airflow_tasks(dag, source_task, source_airflow_task, custom_source_id, airflow_version)
    elif isinstance(task.broker, SequentialBroker):
        for destination in task.destinations:
            task_id = f'{task.task_id}_then_{destination.task_id}'
            airflow_task = make_airflow_task(source_id, destination, custom_task_id=task_id)
            if source_airflow_task is None:
                if airflow_version == 1:
                    dag >> airflow_task
            else:
                source_airflow_task >> airflow_task
            for other_destination in destination.destinations:
                make_airflow_tasks(dag, other_destination, airflow_task, custom_source_id=destination.task_id, airflow_version=airflow_version)
    else:
        airflow_task = make_airflow_task(source_id, task)
        if source_airflow_task is None:
            if airflow_version == 1:
                dag >> airflow_task
        else:
            source_airflow_task >> airflow_task
        for destination in task.destinations:
            make_airflow_tasks(dag, destination, airflow_task)
