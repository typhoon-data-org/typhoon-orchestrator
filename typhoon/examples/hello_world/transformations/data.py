"""Example module with data transformations"""
from io import StringIO, BytesIO
from typing import Union


def to_bytes_buffer(data: Union[StringIO, str, bytes]):
    if isinstance(data, StringIO):
        data = data.getvalue()
    if isinstance(data, str):
        data = data.encode()
    return BytesIO(data)


def buffer_to_string(buffer: Union[StringIO, BytesIO]):
    value = buffer.getvalue()
    if isinstance(value, bytes):
        return value.decode()
    else:
        return value
