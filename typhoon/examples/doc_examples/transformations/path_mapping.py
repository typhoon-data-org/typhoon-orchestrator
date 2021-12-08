"""Example module with data transformations"""
from io import StringIO, BytesIO
from typing import Union
import pandas as pd


def to_regex_name(path: Union[str]) -> str:
    import re
    if isinstance(path, str):
        print(type(path))
        print('adam', path)
        filename = re.findall(r'[^\/]+(?=\.)',path)
        print('dam2', filename[0])
        tablename = re.findall(r'(?:^data_)(.*)(?:_\d\d\d\d-\d\d-\d\dT\d\d_\d\d_\d\d)', filename[0])
        print('vandam',tablename)
    return tablename[0]


