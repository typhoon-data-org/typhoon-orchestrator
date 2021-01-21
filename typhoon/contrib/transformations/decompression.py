import gzip as gz
import zlib as zl
from io import BytesIO
from typing import Union


def gzip(data: Union[bytes, BytesIO]) -> bytes:
    if isinstance(data, bytes):
        data = BytesIO(data)

    with gz.GzipFile(fileobj=data, mode="rb") as f:
        out = f.read()
    return out


def zlib(data: Union[str, bytes, BytesIO]) -> bytes:
    if isinstance(data, BytesIO):
        data = data.getvalue()

    return zl.decompress(data)
