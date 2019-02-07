import functools

from typhoon import config
from typhoon.logger import S3Logger, StdoutLogger, logger_factory


def task_logging_wrapper(bucket, dag_config, task_id, batch_num):
    def decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            custom_logger_type = config.get(dag_config['env'], 'logger')
            custom_logger = logger_factory(custom_logger_type)
            with custom_logger(
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
