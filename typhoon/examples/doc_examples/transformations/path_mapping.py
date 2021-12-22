"""Example module with data transformations"""
from io import StringIO, BytesIO
from typing import Union
import pandas as pd


def to_regex_name(path: Union[str]) -> str:
    import re
    if isinstance(path, str):
        filename = re.findall(r'[^\/]+(?=\.)',path)
        tablename = re.findall(r'(?:^data_)(.*)(?:_\d\d\d\d-\d\d-\d\dT\d\d_\d\d_\d\d)', filename[0])
    return tablename[0]


