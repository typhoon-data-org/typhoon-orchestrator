import functools

from typhoon import config
from typhoon.logger import S3Logger, StdoutLogger, logger_factory, NullLogger


def task_logging_wrapper(bucket, dag_config, task_id, batch_num):
    def decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            custom_logger_type = config.get(dag_config['env'], 'logger', 'None')
            custom_logger = logger_factory(custom_logger_type)
            with custom_logger(
                dag_id=dag_config['dag_id'],
                task_id=task_id,
                ds=dag_config['ds'],
                etl_timestamp=dag_config['etl_timestamp'],
                batch_num=batch_num,
                env=dag_config['env'],
            ):
                with StdoutLogger():
                    try:
                        yield from func(*args, **kwargs)
                    except Exception:
                        # TODO: Mark task as failed in DynamoDB
                        pass

        return func_wrapper

    return decorator
