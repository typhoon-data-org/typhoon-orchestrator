import csv
from io import StringIO
from typing import List


def dicts_to_csv(data: List[dict], delimiter: str = ',', dialect: str = 'unix') -> str:
    """INVARIANT: Assumes that all the dicts have the same keys"""
    if len(data) == 0:
        return ''
    out = StringIO()
    keys = data[0].keys()
    dict_writer = csv.DictWriter(out, keys, delimiter=delimiter, dialect=dialect)
    dict_writer.writeheader()
    dict_writer.writerows(data)
    out.seek(0)
    return out.getvalue()
