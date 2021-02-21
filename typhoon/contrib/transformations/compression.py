import gzip as gz
import zlib as zl
from io import BytesIO
from typing import Union


def gzip(data: Union[str, bytes, BytesIO]) -> BytesIO:
    if isinstance(data, str):
        data = data.encode()
    elif isinstance(data, BytesIO):
        data = data.getvalue()

    out = BytesIO()
    with gz.GzipFile(fileobj=out, mode="wb") as f:
        f.write(data)
    out.seek(0)
    return out


def zlib(data: Union[str, bytes, BytesIO]) -> BytesIO:
    if isinstance(data, str):
        data = data.encode()
    elif isinstance(data, BytesIO):
        data = data.getvalue()

    return BytesIO(zl.compress(data))
