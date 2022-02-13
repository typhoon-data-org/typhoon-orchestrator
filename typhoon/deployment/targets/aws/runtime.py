import os
from dataclasses import dataclass
from typing import Any

import boto3
from importlib_metadata import sys
import jsonpickle
from typhoon.core import DagContext
from typhoon.core.runtime import BrokerInterface, TaskInterface


@dataclass
class LambdaBroker(BrokerInterface):
    dag_id: str

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

        payload_dict = {
            'type': 'task',
            'dag_id': self.dag_id,
            'source_id': source_id,
            'task_name': destination,
            'trigger': 'dag',
            'attempt': 1,
            'batch_num': batch_num,
            'batch': batch,
            'batch_group_id': batch_group_id,
            'dag_context': dag_context.dict(),
        }
        payload = jsonpickle.encode(payload_dict).encode()


        if os.environ.get('INVOKE_LAMBDA_SYNCHRONOUSLY', '').lower() == 'true':
            response = boto3.client('lambda').invoke(
                FunctionName=lambda_function_name,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=payload,
            )
            print(response['LogResult'])
        else:
            boto3.client('lambda').invoke(
                FunctionName=lambda_function_name,
                InvocationType='Event',
                Payload=payload,
            )
