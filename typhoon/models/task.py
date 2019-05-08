import functools

from typhoon import config
from typhoon.logger import StdoutLogger, logger_factory
from typhoon.settings import get_env


def task_logging_wrapper(dag_config, task_id, batch_num):
    def decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            custom_logger_type = config.get(get_env(), 'logger', 'None')
            custom_logger = logger_factory(custom_logger_type)
            print(dag_config)
            with custom_logger(
                dag_id=dag_config['dag_id'],
                task_id=task_id,
                ds=dag_config['ds'],
                etl_timestamp=dag_config['etl_timestamp'],
                batch_num=batch_num,
                env=get_env(),
            ):
                with StdoutLogger():
                    yield from func(*args, **kwargs)

        return func_wrapper

    return decorator
