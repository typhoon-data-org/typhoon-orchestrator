import json
import os
from datetime import datetime
from pathlib import Path

from typhoon.core import make_lambda_payload
from typhoon.core.settings import Settings
from typhoon.handler import handle
from typhoon.core.dags import DagContext


def add_sync_property(func):
    func.sync = func
    return func


@add_sync_property
def foo(a, b, dag_context: DagContext, batch_num):
    return a + b, batch_num, dag_context.execution_date


def test_handle_task(monkeypatch):
    Settings.typhoon_home = Path(__file__).parent

    payload = make_lambda_payload(
        dag_name='handler_test',
        task_name='foo',
        args=(3, 2),
        kwargs={
            'dag_context': DagContext.parse_obj({
                'start_interval': '2019-08-15 00:00:00',
                'end_interval': '2019-08-16 00:00:00'
            }),
            'batch_num': 4
        },
    )

    assert handle(json.loads(payload.decode()), None) == (5, 4, datetime(2019, 8, 16))
