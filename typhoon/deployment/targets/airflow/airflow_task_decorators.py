import functools
import json
from typing import Iterable

from typing_extensions import Protocol


class AirflowEdgeFunc(Protocol):
    def __call__(self, tid: str, data: Iterable, context: dict):
        ...


def xcom_input(func: AirflowEdgeFunc):
    @functools.wraps(func)
    def wrapper(tid: str, **context):
        data = json.loads(context['ti'].xcom_pull(task_ids=tid, key='result'))
        return func(tid, data, context)
    return wrapper


def xcom_output(func):
    @functools.wraps(func)
    def wrapper(tid: str, **context):
        output = func(tid, **context)
        context['ti'].xcom_push('result', json.dumps(list(output)))
    return wrapper

