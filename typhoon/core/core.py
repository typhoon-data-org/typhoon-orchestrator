import os
from functools import wraps
from typing import Optional, Dict, Tuple, Any

import boto3
import jsonpickle

# Define sentinels
SKIP_BATCH = object()


def task(
        asynchronous: bool,
        dag_name: str,
        remote_aws_lambda_function_name: Optional[str] = None,
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
                )

                boto3.client('lambda').invoke(
                    FunctionName=lambda_function_name,
                    InvocationType='Event',  # makes the call async
                    Payload=payload,
                )
            else:   # Running locally
                return func(*args, **kwargs)

        assert asynchronous
        async_wrapper.sync = func       # Hat tip to zappa. Avoids infinite loop
        return async_wrapper

    return task_decorator


def make_lambda_payload(dag_name: str, task_name: str, args: Tuple[Any, ...], kwargs: Dict):
    kwargs_encoded_context = kwargs.copy()
    kwargs_encoded_context['dag_context'] = kwargs_encoded_context['dag_context'].dict()
    payload_dict = {
        'type': 'task',
        'dag_name': dag_name,
        'task_name': task_name,
        'trigger': 'dag',   # Can also be manual or scheduler
        'attempt': 1,       # If it fails then the scheduler will retry with attempt 2 (if retries are defined)
        'args': args,
        'kwargs': kwargs_encoded_context,
    }
    return jsonpickle.encode(payload_dict).encode()


def get_func_task_name(func):
    """
    Format the task function name via inspection.
    """
    return func.__name__
