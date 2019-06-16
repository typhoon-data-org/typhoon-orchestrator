import io
import logging
import os
import sys
from typing import ContextManager, Type, Optional

# from typhoon.aws import write_logs
from typhoon.core import get_typhoon_config
from typhoon.logger_interface import LoggingInterface


class LoggingContext(object):
    handlers: dict = {}

    def __init__(self, handler, handler_name, logger=None, level=logging.INFO, close=True):
        self.handler = handler
        self.handler_name = handler_name
        self.logger = logger or logging.getLogger()  # Root logger if not defined
        self.level = level
        self.close = close
        self.old_handler = None

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler_name in self.handlers.keys():
            self.old_handler = self.handlers[self.handler_name]
            self.logger.removeHandler(self.old_handler)
        self.handlers[self.handler_name] = self.handler
        self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()
        if self.old_handler:
            self.handlers[self.handler_name] = self.old_handler
            self.logger.addHandler(self.old_handler)


class StdoutLogger(ContextManager):
    def __enter__(self):
        self.handler = logging.StreamHandler(sys.stdout)   # Log to stdout
        self.logging_context = LoggingContext(handler=self.handler, handler_name='stdout')
        self.logging_context.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logging_context.__exit__(exc_type, exc_val, exc_tb)


class FileLogger(LoggingInterface):
    def __init__(self, dag_id, task_id, ds: str, etl_timestamp: str, batch_num, env):
        config = get_typhoon_config()
        self.log_buffer = io.StringIO()
        self.log_file = os.path.join(
            config.local_log_path,
            f'{dag_id}/{ds.replace("-", "_")}/execution{etl_timestamp.replace(":", "_").replace("-", "_")}/'
            f'{ task_id }_{batch_num}.log'
        )

    def __enter__(self):
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        self.handler = logging.FileHandler(self.log_file)
        self.logging_context = LoggingContext(handler=self.handler, handler_name='file')
        self.logging_context.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logging_context.__exit__(exc_type, exc_val, exc_tb)


class S3Logger(LoggingInterface):
    def __init__(self, dag_id, task_id, ds, etl_timestamp, batch_num, env):
        config = get_typhoon_config()
        self.bucket = config.s3_bucket
        self.log_buffer = io.StringIO()
        self.key = f'logs/{dag_id}/{ds}/execution{etl_timestamp}/{ task_id }_{batch_num}.log'

    def __enter__(self):
        self.handler = logging.StreamHandler(self.log_buffer)
        self.logging_context = LoggingContext(handler=self.handler, handler_name='s3')
        self.logging_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # write_logs(
        #     self.log_buffer.getvalue(),
        #     bucket=self.bucket,
        #     key=self.key
        # )
        self.logging_context.__exit__(exc_type, exc_val, exc_tb)


class NullLogger(LoggingInterface):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def logger_factory(logger_type: Optional[str]) -> Type[LoggingInterface]:
    # TODO: Add logging to elasticsearch
    if logger_type == 's3':
        return S3Logger
    if logger_type == 'file':
        return FileLogger
    if logger_type == 'None' or logger_type is None:
        return NullLogger
    else:
        raise ValueError(f'Logger {logger_type} is not a valid option')
