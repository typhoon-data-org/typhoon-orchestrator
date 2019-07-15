import json
import os
from datetime import datetime
from pathlib import Path

import mock

from typhoon.core import make_lambda_payload
from typhoon.handler import handle
import typhoon.settings

DAG_CONFIG = {}


def foo(a, b, batch_num):
    return a + b, batch_num, DAG_CONFIG['execution_date']


def test_handle_task(monkeypatch):
    os.environ['TYPHOON_HOME'] = str(Path(__file__).parent)

    payload = make_lambda_payload(
        dag_name='handler_test',
        task_name='foo',
        args=(3, 2),
        kwargs={'batch_num': 4},
        execution_context={
            'ds': '2019-08-16',
            'execution_date': datetime(2019, 8, 16),
        }
    )

    assert handle(None, json.loads(payload.decode())) == (5, 4, datetime(2019, 8, 16))
