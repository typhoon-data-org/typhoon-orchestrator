import csv
import json
from io import StringIO
from typing import List, Optional, Union


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


def csv_to_dicts(data: Union[str, bytes], fieldnames: Optional[List[str]] = None, delimiter: str = ',', dialect: str = 'unix') -> List[dict]:
    if isinstance(data, bytes):
        data = data.decode()
    result = []
    dict_reader = csv.DictReader(StringIO(data), fieldnames=fieldnames, delimiter=delimiter, dialect=dialect)
    for row in dict_reader:
        result.append(row)
    return result


def to_dataframe(data):
    import pandas as pd
    return pd.DataFrame(data)


def json_array_to_json_records(data: list) -> str:
    result = [json.dumps(x) for x in data]
    return '\n'.join(result)

def json_dumps(batch) -> str:
    return json.dumps(batch)