import functools
import io
import logging
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


def node(bucket, dag_id, task_id, ds):
    def decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            log_buffer = io.StringIO()
            try:
                root = logging.getLogger()
                handler = logging.StreamHandler(log_buffer)
                root.addHandler(handler)
                # root.setLevel(settings.LOGGING_LEVEL)

                return func(*args, **kwargs)
            except Exception as e:
                logging.error(e)
            finally:
                now = datetime.now()
                write_logs(
                    log_buffer.getvalue(),
                    bucket=bucket,
                    key=f'logs/{dag_id}/{ds}/{task_id}_{now}.log'
                )

        return func_wrapper

    return decorator
