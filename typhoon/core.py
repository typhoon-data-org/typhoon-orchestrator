import json
import os
from functools import wraps
from typing import Optional, Dict, Tuple, Any

import boto3

from typhoon.config import CLIConfig, TyphoonConfig
from typhoon.settings import get_env

# Define sentinels
SKIP_BATCH = object()


def get_typhoon_config(use_cli_config: Optional[bool] = False, target_env: Optional[str] = None):
    """Reads the Typhoon Config file.
    If no target_env is specified it uses the TYPHOON-ENV environment variable.
    """
    env = target_env or get_env()
    return CLIConfig(env) if use_cli_config else TyphoonConfig(env)


def task(
        asynchronous: bool,
        dag_name: str,
        remote_aws_lambda_function_name: Optional[str] = None,
        execution_context: Optional[Dict] = None,
):
    def task_decorator(func):
        if not asynchronous:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return sync_wrapper

        task_path = get_func_task_name(func)

        @wraps(func)
        def async_wrapper(*args, **kwargs):
            lambda_function_name = remote_aws_lambda_function_name or os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

            if lambda_function_name:
                payload = make_lambda_payload(
                    dag_name=dag_name,
                    task_name=task_path,
                    args=args,
                    kwargs=kwargs,
                    execution_context=execution_context,
                )

                boto3.client('lambda').invoke(
                    FunctionName=lambda_function_name,
                    InvocationType='Event',  # makes the call async
                    Payload=payload,
                )
            else:   # Running locally
                return func(*args, **kwargs)

        assert asynchronous
        return async_wrapper

    return task_decorator


def make_lambda_payload(dag_name: str, task_name: str, args: Tuple[Any, ...], kwargs: Dict, execution_context: Optional[Dict]):
    context = execution_context.copy()
    context['execution_date'] = str(execution_context['execution_date'])
    payload_dict = {
        'type': 'task',
        'dag_name': dag_name,
        'task_name': task_name,
        'trigger': 'dag',   # Can also be manual or scheduler
        'attempt': 1,       # If it fails then the scheduler will retry with attempt 2 (if retries are defined)
        'execution_context': context or {},
        'args': args,
        'kwargs': kwargs,
    }
    return json.dumps(payload_dict).encode()


def get_func_task_name(func):
    """
    Format the task function name via inspection.
    """
    return func.__name__
