import functools

from zappa import async

from typhoon.logger import S3Logger, StdoutLogger


def task(asynchronous=True):
    def decorator(func):
        @functools.wraps(func)
        def normal_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        @functools.wraps(func)
        @async.task
        def async_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        if asynchronous:
            return async_wrapper
        else:
            return normal_wrapper

    return decorator


def task_logging_wrapper(bucket, dag_config, task_id, batch_num):
    def decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            with S3Logger(
                bucket=bucket,
                dag_id=dag_config['dag_id'],
                task_id=task_id,
                ds=dag_config['ds'],
                etl_timestamp=dag_config['etl_timestamp'],
                batch_num=batch_num,
            ):
                with StdoutLogger():
                    yield from func(*args, **kwargs)

        return func_wrapper

    return decorator
