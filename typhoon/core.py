# Define sentinels
import inspect
import json
import os
from functools import wraps
from typing import Optional, Dict, Tuple, Any

import boto3

from typhoon.config import CLIConfig, TyphoonConfig
from typhoon.settings import get_env

SKIP_BATCH = object()


def get_typhoon_config(use_cli_config: Optional[bool] = False, target_env: Optional[str] = None):
    """Reads the Typhoon Config file.
    If no target_env is specified it uses the TYPHOON-ENV environment variable.
    """
    env = target_env or get_env()
    return CLIConfig(env) if use_cli_config else TyphoonConfig(env)


def task(
        asynchronous: bool,
        env: Optional[str] = None,
        remote_aws_lambda_function_name: Optional[str] = None,
        remote_aws_region: Optional[str] = None,
        execution_context: Optional[Dict] = None,
):
    def task_decorator(func):
        if not asynchronous:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return sync_wrapper

        task_path = get_func_task_path(func)
        environment = env or os.environ['TYPHOON-ENV']

        @wraps(func)
        def async_wrapper(*args, **kwargs):
            lambda_function_name = remote_aws_lambda_function_name or os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
            aws_region = remote_aws_region or os.environ.get('AWS_REGION')

            if lambda_function_name:
                payload = make_lambda_payload(
                    env=environment,
                    task_path=task_path,
                    args=args,
                    kwargs=kwargs,
                    execution_context=execution_context,
                )

                boto3.Session(region_name=aws_region, profile_name='new-aws').client('lambda').invoke(
                    FunctionName=lambda_function_name,
                    InvocationType='Event',  # makes the call async
                    Payload=payload,
                )
            else:   # Running locally
                return func(*args, **kwargs)

        assert asynchronous
        return async_wrapper

    return task_decorator


def make_lambda_payload(env: str, task_path: str, args: Tuple[Any, ...], kwargs: Dict, execution_context: Optional[Dict]):
    context = execution_context.copy()
    context['execution_date'] = str(execution_context['execution_date'])
    payload_dict = {
        'environment': env,
        'task_path': task_path,
        'trigger': 'dag',   # Can also be manual or scheduler
        'attempt': 1,       # If it fails then the scheduler will retry with attempt 2 (if retries are defined)
        'execution_context': context or {},
        'args': args,
        'kwargs': kwargs,
    }
    return json.dumps(payload_dict).encode()


def get_func_task_path(func):
    """
    Format the modular task path for a function via inspection.
    """
    module_path = inspect.getmodule(func).__name__
    task_path = '{module_path}.{func_name}'.format(
        module_path=module_path,
        func_name=func.__name__
    )
    return task_path


@task(True, env='test', remote_aws_lambda_function_name='testAsync', remote_aws_region='eu-west-1', execution_context={'ds': '2018-12-03'})
def test_function():
    print('test_function')


if __name__ == '__main__':
    test_function()
    a = 2
