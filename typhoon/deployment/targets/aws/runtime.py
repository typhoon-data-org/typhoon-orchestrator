import base64
import json
import os
from dataclasses import dataclass
from typing import Any

import boto3
try:
    from importlib.metadata import sys
except ImportError:  # Python < 3.10 (backport)
    from importlib_metadata import sys
import jsonpickle
from typhoon.core import DagContext
from typhoon.core.runtime import BrokerInterface, TaskInterface


@dataclass
class LambdaBroker(BrokerInterface):
    dag_id: str

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

    def send(
            self,
            dag_context: DagContext,
            source_id: str,
            destination: TaskInterface,
            batch_num: int,
            batch: Any,
            batch_group_id: str,
    ):
        lambda_function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
        if lambda_function_name is None:
            print('You are trying to invoke a lambda function locally. Make sure to set the env variable AWS_LAMBDA_FUNCTION_NAME')
            sys.exit(1)

        invoke_lambda_synchronously = os.environ.get('INVOKE_LAMBDA_SYNCHRONOUSLY', '').lower() == 'true'
        payload_dict = {
            'type': 'task',
            'dag_id': self.dag_id,
            'source_id': source_id,
            'task_name': destination.task_id,
            'trigger': 'dag',
            'attempt': 1,
            'batch_num': batch_num,
            'batch': batch,
            'batch_group_id': batch_group_id,
            'dag_context': dag_context.dict(),
            'invoke_lambda_synchronously': invoke_lambda_synchronously,
        }
        payload = jsonpickle.encode(payload_dict).encode()


        if invoke_lambda_synchronously:
            response = boto3.client('lambda').invoke(
                FunctionName=lambda_function_name,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=payload,
            )
            print(base64.b64decode(response['LogResult']).decode())
        else:
            boto3.client('lambda').invoke(
                FunctionName=lambda_function_name,
                InvocationType='Event',
                Payload=payload,
            )


def get_payload_from_event(event):
    return jsonpickle.decode(json.dumps(event))
