import functools
import io
import logging
import sys
from datetime import datetime

from zappa import async


# {
#     'version': '0',
#     'id': 'b625a64f-41a0-8b6f-3153-aa62c3f898e2',
#     'detail-type': 'Scheduled Event',
#     'source': 'aws.events',
#     'account': '082844907076',
#     'time': '2019-02-05T16:52:09Z',
#     'region': 'eu-west-1',
#     'resources': ['arn:aws:events:eu-west-1:082844907076:rule/two_minutes'],
#     'detail': {}
# }
from typhoon.aws import write_logs


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
            log_buffer = io.StringIO()
            try:
                root = logging.getLogger()
                if len(root.handlers) < 2:          # Avoid duplicate logging
                    handler = logging.StreamHandler(sys.stdout)   # Log to stdout
                    root.addHandler(handler)
                handler = logging.StreamHandler(log_buffer)
                root.addHandler(handler)
                root.setLevel(logging.INFO)

                yield from func(*args, **kwargs)
            except Exception as e:
                logging.error(e)
            finally:
                dag_id = dag_config['dag_id']
                ds = dag_config['ds']
                etl_timestamp = dag_config['etl_timestamp']
                write_logs(
                    log_buffer.getvalue(),
                    bucket=bucket,
                    key=f'logs/{dag_id}/{ds}/execution{etl_timestamp}/{ task_id }_{batch_num}.log'
                )

        return func_wrapper

    return decorator
