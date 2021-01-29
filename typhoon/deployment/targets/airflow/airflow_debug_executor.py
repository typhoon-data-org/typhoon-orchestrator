#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
DebugExecutor
.. seealso::
    For more information on how the DebugExecutor works, take a look at the guide:
    :ref:`executor:DebugExecutor`
"""
import contextlib
import copy
import os
import re
import signal
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, NamedTuple, Union, Iterable

from airflow import DAG
from airflow.configuration import conf, log
from airflow.exceptions import AirflowSkipException, AirflowException, AirflowTaskTimeout
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance, BaseOperator, Log
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.net import get_hostname
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from sqlalchemy.exc import OperationalError


class TaskInstanceKey(NamedTuple):
    """Key used to identify task instance."""

    dag_id: str
    task_id: str
    execution_date: datetime
    try_number: int

    @property
    def primary(self) -> Tuple[str, str, datetime]:
        """Return task instance primary key part of the key"""
        return self.dag_id, self.task_id, self.execution_date

    @property
    def reduced(self) -> 'TaskInstanceKey':
        """Remake the key by subtracting 1 from try number to match in memory information"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.execution_date, max(1, self.try_number - 1))

    def with_try_number(self, try_number: int) -> 'TaskInstanceKey':
        """Returns TaskInstanceKey with provided ``try_number``"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.execution_date, try_number)


class DebugExecutor(BaseExecutor):
    """
    This executor is meant for debugging purposes. It can be used with SQLite.
    It executes one task instance at time. Additionally to support working
    with sensors, all sensors ``mode`` will be automatically set to "reschedule".
    """

    _terminated = threading.Event()

    def __init__(self):
        super().__init__()
        self.running = set()
        self.tasks_to_run: List[TaskInstance] = []
        # Place where we keep information for task instance raw run
        self.tasks_params: Dict[TaskInstanceKey, Dict[str, Any]] = {}
        # self.fail_fast = conf.getboolean("debug", "fail_fast")
        self.fail_fast = True

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs."""
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
            open_slots = self.parallelism - len(self.running)

        num_running_tasks = len(self.running)
        num_queued_tasks = len(self.queued_tasks)

        self.log.debug("%s running task instances", num_running_tasks)
        self.log.debug("%s in queue", num_queued_tasks)
        self.log.debug("%s open slots", open_slots)

        self.trigger_tasks(open_slots)

        # Calling child class sync method
        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

    def execute_async(self, *args, **kwargs) -> None:  # pylint: disable=signature-differs
        """The method is replaced by custom trigger_task implementation."""

    def sync(self) -> None:
        task_succeeded = True
        while self.tasks_to_run:
            ti = self.tasks_to_run.pop(0)
            if self.fail_fast and not task_succeeded:
                self.log.info("Setting %s to %s", ti.key, State.UPSTREAM_FAILED)
                ti.set_state(State.UPSTREAM_FAILED)
                self.change_state(ti.key, State.UPSTREAM_FAILED)
                continue

            if self._terminated.is_set():
                self.log.info("Executor is terminated! Stopping %s to %s", ti.key, State.FAILED)
                ti.set_state(State.FAILED)
                self.change_state(ti.key, State.FAILED)
                _run_finished_callback(ti)  # pylint: disable=protected-access
                continue

            task_succeeded = self._run_task(ti)

    def _run_task(self, ti: TaskInstance) -> bool:
        self.log.debug("Executing task: %s", ti)
        key = ti.key
        try:
            params = self.tasks_params.pop(ti.key, {})
            _run_raw_task(ti, job_id=ti.job_id, **params)  # pylint: disable=protected-access
            self.change_state(key, State.SUCCESS)
            _run_finished_callback(ti)  # pylint: disable=protected-access
            return True
        except Exception as e:  # pylint: disable=broad-except
            ti.set_state(State.FAILED)
            self.change_state(key, State.FAILED)
            _run_finished_callback(ti)  # pylint: disable=protected-access
            self.log.exception("Failed to execute task: %s.", str(e))
            return False

    def queue_task_instance(
        self,
        task_instance: TaskInstance,
        mark_success: bool = False,
        pickle_id: Optional[str] = None,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        pool: Optional[str] = None,
        cfg_path: Optional[str] = None,
    ) -> None:
        """Queues task instance with empty command because we do not need it."""
        self.queue_command(
            task_instance,
            [str(task_instance)],  # Just for better logging, it's not used anywhere
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue,
        )
        # Save params for TaskInstance._run_raw_task
        self.tasks_params[task_instance.key] = {
            "mark_success": mark_success,
            "pool": pool,
        }

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Triggers tasks. Instead of calling exec_async we just
        add task instance to tasks_to_run queue.
        :param open_slots: Number of open slots
        """
        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],  # pylint: disable=unnecessary-comprehension
            key=lambda x: x[1][1],
            reverse=True,
        )
        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (_, _, _, ti) = sorted_queue.pop(0)
            self.queued_tasks.pop(key)
            self.running.add(key)
            self.tasks_to_run.append(ti)  # type: ignore

    def end(self) -> None:
        """
        When the method is called we just set states of queued tasks
        to UPSTREAM_FAILED marking them as not executed.
        """
        for ti in self.tasks_to_run:
            self.log.info("Setting %s to %s", ti.key, State.UPSTREAM_FAILED)
            ti.set_state(State.UPSTREAM_FAILED)
            self.change_state(ti.key, State.UPSTREAM_FAILED)

    def terminate(self) -> None:
        self._terminated.set()

    def change_state(self, key: TaskInstanceKey, state: str, info=None) -> None:
        self.log.debug("Popping %s from executor task queue.", key)
        self.running.remove(key)
        self.event_buffer[key] = state, info


@provide_session
def _run_raw_task(
    self,
    mark_success: bool = False,
    test_mode: bool = False,
    job_id: Optional[str] = None,
    pool: Optional[str] = None,
    error_file: Optional[str] = None,
    session=None,
) -> None:
    """
    Immediately runs the task (without checking or changing db state
    before execution) and then sets the appropriate final state after
    completion and runs any post-execute callbacks. Meant to be called
    only after another function changes the state to running.
    :param mark_success: Don't run the task, mark its state as success
    :type mark_success: bool
    :param test_mode: Doesn't record success or failure in the DB
    :type test_mode: bool
    :param pool: specifies the pool to use to run the task instance
    :type pool: str
    :param session: SQLAlchemy ORM Session
    :type session: Session
    """
    task = self.task
    self.test_mode = test_mode
    refresh_from_task(self, task, pool_override=pool)
    # self.refresh_from_db(session=session)
    self.job_id = job_id
    self.hostname = get_hostname()

    context = {}  # type: Dict
    actual_start_date = timezone.utcnow()
    try:
        if not mark_success:
            context = self.get_template_context()
            _prepare_and_execute_task_with_callbacks(self, context, task)
        self.refresh_from_db(lock_for_update=True)
        self.state = State.SUCCESS
    except AirflowSkipException as e:
        # Recording SKIP
        # log only if exception has any arguments to prevent log flooding
        if e.args:
            self.log.info(e)
        self.refresh_from_db(lock_for_update=True)
        self.state = State.SKIPPED
        self.log.info(
            'Marking task as SKIPPED. '
            'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
            self.dag_id,
            self.task_id,
            _date_or_empty(self, 'execution_date'),
            _date_or_empty(self, 'start_date'),
            _date_or_empty(self, 'end_date'),
        )
    # except AirflowRescheduleException as reschedule_exception:
    #     self.refresh_from_db()
    #     self._handle_reschedule(actual_start_date, reschedule_exception, test_mode)
    #     return
    # except AirflowFailException as e:
    #     self.refresh_from_db()
    #     self.handle_failure(e, test_mode, force_fail=True, error_file=error_file)
    #     raise
    except AirflowException as e:
        self.refresh_from_db()
        # for case when task is marked as success/failed externally
        # current behavior doesn't hit the success callback
        if self.state in {State.SUCCESS, State.FAILED}:
            return
        else:
            self.handle_failure(e, test_mode, error_file=error_file)
            raise
    except (Exception, KeyboardInterrupt) as e:
        self.handle_failure(e, test_mode, error_file=error_file)
        raise

    # Recording SUCCESS
    self.end_date = timezone.utcnow()
    self.log.info(
        'Marking task as SUCCESS. '
        'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
        self.dag_id,
        self.task_id,
        _date_or_empty(self, 'execution_date'),
        _date_or_empty(self, 'start_date'),
        _date_or_empty(self, 'end_date'),
    )
    self.set_duration()
    if not test_mode:
        session.add(Log(self.state, self))
        session.merge(self)

    session.commit()

    if not test_mode:
        _run_mini_scheduler_on_child_tasks(self, session)


def _run_finished_callback(self, error: Optional[Union[str, Exception]] = None) -> None:
    """
    Call callback defined for finished state change.
    NOTE: Only invoke this function from caller of self._run_raw_task or
    self.run
    """
    if self.state == State.FAILED:
        task = self.task
        if task.on_failure_callback is not None:
            context = self.get_template_context()
            context["exception"] = error
            task.on_failure_callback(context)
    elif self.state == State.SUCCESS:
        task = self.task
        if task.on_success_callback is not None:
            context = self.get_template_context()
            task.on_success_callback(context)
    elif self.state == State.UP_FOR_RETRY:
        task = self.task
        if task.on_retry_callback is not None:
            context = self.get_template_context()
            context["exception"] = error
            task.on_retry_callback(context)


def refresh_from_task(self, task, pool_override=None):
    """
    Copy common attributes from the given task.
    :param task: The task object to copy from
    :type task: airflow.models.BaseOperator
    :param pool_override: Use the pool_override instead of task's pool
    :type pool_override: str
    """
    self.queue = task.queue
    self.pool = pool_override or task.pool
    # self.pool_slots = task.pool_slots
    self.priority_weight = task.priority_weight_total
    self.run_as_user = task.run_as_user
    self.max_tries = task.retries
    self.executor_config = task.executor_config
    self.operator = task.task_type


def _prepare_and_execute_task_with_callbacks(self, context, task):
    """Prepare Task for Execution"""
    # from airflow.models.renderedtifields import RenderedTaskInstanceFields

    task_copy = prepare_for_execution(task)
    self.task = task_copy

    def signal_handler(signum, frame):  # pylint: disable=unused-argument
        self.log.error("Received SIGTERM. Terminating subprocesses.")
        task_copy.on_kill()
        raise AirflowException("Task received SIGTERM signal")

    signal.signal(signal.SIGTERM, signal_handler)

    # Don't clear Xcom until the task is certain to execute
    self.clear_xcom_data()
    # self.render_templates(context=context)
    self.render_templates()
    # RenderedTaskInstanceFields.write(RenderedTaskInstanceFields(ti=self, render_templates=False))
    # RenderedTaskInstanceFields.delete_old_records(self.task_id, self.dag_id)

    # Export context to make it available for operators to use.
    # airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
    airflow_context_vars = context_to_airflow_vars(context)
    self.log.info(
        "Exporting the following env vars:\n%s",
        '\n'.join([f"{k}={v}" for k, v in airflow_context_vars.items()]),
    )

    os.environ.update(airflow_context_vars)

    # Run pre_execute callback
    task_copy.pre_execute(context=context)

    # Run on_execute callback
    # self._run_execute_callback(context, task)

    # if task_copy.is_smart_sensor_compatible():
    #     # Try to register it in the smart sensor service.
    #     registered = False
    #     try:
    #         registered = task_copy.register_in_sensor_service(self, context)
    #     except Exception as e:
    #         self.log.warning(
    #             "Failed to register in sensor service.Continue to run task in non smart sensor mode."
    #         )
    #         self.log.exception(e, exc_info=True)
    #
    #         # if registered:
    #         #     # Will raise AirflowSmartSensorException to avoid long running execution.
    #         #     self._update_ti_state_for_sensing()

    # Execute the task
    with set_current_context(context):
        result = _execute_task(self, context, task_copy)

    # Run post_execute callback
    task_copy.post_execute(context=context, result=result)


def prepare_for_execution(self) -> "BaseOperator":
    """
    Lock task for execution to disable custom action in __setattr__ and
    returns a copy of the task
    """
    other = copy.copy(self)
    other._lock_for_execution = True  # pylint: disable=protected-access
    # Sensors in `poke` mode can block execution of DAGs when running
    # with single process executor, thus we change the mode to`reschedule`
    # to allow parallel task being scheduled and executed
    if isinstance(self, BaseSensorOperator):
        self.log.warning("DebugExecutor changes sensor mode to 'reschedule'.")
        other.mode = 'reschedule'
    return other


def _run_execute_callback(self, context, task):
    """Functions that need to be run before a Task is executed"""
    try:
        if task.on_execute_callback:
            task.on_execute_callback(context)
    except Exception as exc:  # pylint: disable=broad-except
        self.log.error("Failed when executing execute callback")
        self.log.exception(exc)


_CURRENT_CONTEXT = []
XCOM_RETURN_KEY = 'return_value'


@contextlib.contextmanager
def set_current_context(context):
    """
    Sets the current execution context to the provided context object.
    This method should be called once per Task execution, before calling operator.execute.
    """
    _CURRENT_CONTEXT.append(context)
    try:
        yield context
    finally:
        expected_state = _CURRENT_CONTEXT.pop()
        if expected_state != context:
            log.warning(
                "Current context is not equal to the state at context stack. Expected=%s, got=%s",
                context,
                expected_state,
            )


def _execute_task(self, context, task_copy):
    """Executes Task (optionally with a Timeout) and pushes Xcom results"""
    # If a timeout is specified for the task, make it fail
    # if it goes beyond
    if task_copy.execution_timeout:
        try:
            with timeout(task_copy.execution_timeout.total_seconds()):
                result = task_copy.execute(context=context)
        except AirflowTaskTimeout:
            task_copy.on_kill()
            raise
    else:
        result = task_copy.execute(context=context)
    # If the task returns a result, push an XCom containing it
    # if task_copy.do_xcom_push and result is not None:
    #     self.xcom_push(key=XCOM_RETURN_KEY, value=result)
    return result


def _date_or_empty(self, attr):
    if hasattr(self, attr):
        date = getattr(self, attr)
        if date:
            return date.strftime('%Y%m%dT%H%M%S')
    return ''


@provide_session
def _run_mini_scheduler_on_child_tasks(self, session=None) -> None:
    from airflow.models import DagRun  # Avoid circular import

    try:
        # Re-select the row with a lock
        dag_run = with_row_locks(
            session.query(DagRun).filter_by(
                dag_id=self.dag_id,
                execution_date=self.execution_date,
            )
        ).one()

        # Get a partial dag with just the specific tasks we want to
        # examine. In order for dep checks to work correctly, we
        # include ourself (so TriggerRuleDep can check the state of the
        # task we just executed)
        partial_dag = partial_subset(
            self.task.dag,
            self.task.downstream_task_ids,
            include_downstream=False,
            include_upstream=False,
            include_direct_upstream=True,
        )

        dag_run.dag = partial_dag
        info = dag_run.task_instance_scheduling_decisions(session)

        skippable_task_ids = {
            task_id
            for task_id in partial_dag.task_ids
            if task_id not in self.task.downstream_task_ids
        }

        schedulable_tis = [ti for ti in info.schedulable_tis if ti.task_id not in skippable_task_ids]
        for schedulable_ti in schedulable_tis:
            if not hasattr(schedulable_ti, "task"):
                schedulable_ti.task = self.task.dag.get_task(schedulable_ti.task_id)

        num = dag_run.schedule_tis(schedulable_tis)
        self.log.info("%d downstream tasks scheduled from follow-on schedule check", num)

        session.commit()
    except OperationalError as e:
        # Any kind of DB error here is _non fatal_ as this block is just an optimisation.
        self.log.info(
            f"Skipping mini scheduling run due to exception: {e.statement}",
            exc_info=True,
        )
        session.rollback()


USE_ROW_LEVEL_LOCKING = True


def with_row_locks(query, **kwargs):
    """
    Apply with_for_update to an SQLAlchemy query, if row level locking is in use.
    :param query: An SQLAlchemy Query object
    :param kwargs: Extra kwargs to pass to with_for_update (of, nowait, skip_locked, etc)
    :return: updated query
    """
    if USE_ROW_LEVEL_LOCKING:
        return query.with_for_update(**kwargs)
    else:
        return query


try:
    from re import Pattern as PatternType  # type: ignore
except ImportError:
    PatternType = type(re.compile('', 0))


def partial_subset(
    self: DAG,
    task_ids_or_regex: Union[str, PatternType, Iterable[str]],
    include_downstream=False,
    include_upstream=True,
    include_direct_upstream=False,
):
    """
    Returns a subset of the current dag as a deep copy of the current dag
    based on a regex that should match one or many tasks, and includes
    upstream and downstream neighbours based on the flag passed.
    :param task_ids_or_regex: Either a list of task_ids, or a regex to
        match against task ids (as a string, or compiled regex pattern).
    :type task_ids_or_regex: [str] or str or re.Pattern
    :param include_downstream: Include all downstream tasks of matched
        tasks, in addition to matched tasks.
    :param include_upstream: Include all upstream tasks of matched tasks,
        in addition to matched tasks.
    """
    # deep-copying self.task_dict and self._task_group takes a long time, and we don't want all
    # the tasks anyway, so we copy the tasks manually later
    task_dict = self.task_dict
    task_group = self._task_group
    self.task_dict = {}
    self._task_group = None  # type: ignore
    dag = copy.deepcopy(self)
    self.task_dict = task_dict
    self._task_group = task_group

    if isinstance(task_ids_or_regex, (str, PatternType)):
        matched_tasks = [t for t in self.tasks if re.findall(task_ids_or_regex, t.task_id)]
    else:
        matched_tasks = [t for t in self.tasks if t.task_id in task_ids_or_regex]

    also_include = []
    for t in matched_tasks:
        if include_downstream:
            also_include += t.get_flat_relatives(upstream=False)
        if include_upstream:
            also_include += t.get_flat_relatives(upstream=True)
        elif include_direct_upstream:
            also_include += t.upstream_list

    # Compiling the unique list of tasks that made the cut
    # Make sure to not recursively deepcopy the dag while copying the task
    dag.task_dict = {
        t.task_id: copy.deepcopy(t, {id(t.dag): dag})  # type: ignore
        for t in matched_tasks + also_include
    }

    def filter_task_group(group, parent_group):
        """Exclude tasks not included in the subdag from the given TaskGroup."""
        copied = copy.copy(group)
        copied.used_group_ids = set(copied.used_group_ids)
        copied._parent_group = parent_group

        copied.children = {}

        for child in group.children.values():
            if isinstance(child, BaseOperator):
                if child.task_id in dag.task_dict:
                    copied.children[child.task_id] = dag.task_dict[child.task_id]
            else:
                filtered_child = filter_task_group(child, copied)

                # Only include this child TaskGroup if it is non-empty.
                if filtered_child.children:
                    copied.children[child.group_id] = filtered_child

        return copied

    dag._task_group = filter_task_group(self._task_group, None)

    # Removing upstream/downstream references to tasks and TaskGroups that did not make
    # the cut.
    subdag_task_groups = dag.task_group.get_task_group_dict()
    for group in subdag_task_groups.values():
        group.upstream_group_ids = group.upstream_group_ids.intersection(subdag_task_groups.keys())
        group.downstream_group_ids = group.downstream_group_ids.intersection(subdag_task_groups.keys())
        group.upstream_task_ids = group.upstream_task_ids.intersection(dag.task_dict.keys())
        group.downstream_task_ids = group.downstream_task_ids.intersection(dag.task_dict.keys())

    for t in dag.tasks:
        # Removing upstream/downstream references to tasks that did not
        # make the cut
        t._upstream_task_ids = t.upstream_task_ids.intersection(dag.task_dict.keys())
        t._downstream_task_ids = t.downstream_task_ids.intersection(dag.task_dict.keys())

    if len(dag.tasks) < len(self.tasks):
        dag.partial = True

    return dag
