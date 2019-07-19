import functools

from typhoon.core import get_typhoon_config
from typhoon.logger import StdoutLogger, logger_factory
from typhoon.models.dag_context import DagContext
from typhoon.settings import get_env


def task_logging_wrapper(dag_id: str, dag_context: DagContext, task_id, batch_num):
    def decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            config = get_typhoon_config()
            custom_logger_type = config.logger
            custom_logger = logger_factory(custom_logger_type)
            # print(dag_context)
            with custom_logger(
                dag_id=dag_id,
                task_id=task_id,
                ds=dag_context.ds,
                etl_timestamp=dag_context.etl_timestamp,
                batch_num=batch_num,
                env=get_env(),
            ):
                with StdoutLogger():
                    yield from func(*args, **kwargs)

        return func_wrapper

    return decorator
