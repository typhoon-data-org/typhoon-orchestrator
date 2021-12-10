"""Example module with data transformations"""
from io import StringIO, BytesIO
from typing import Union, List
import pandas as pd
import simplejson as json


def data_lake_path(date_string: str, system: str, part: int, extension: str) -> str:
    return f'/data/{system}/{date_string}/part{part}.{extension}'


def json_loads_to_dict(data: Union[str, bytes]) -> dict:
    import json
    return json.loads(data)


def df_add_country_median(data: pd.DataFrame) -> pd.DataFrame:
    df = data.groupby('Country').mean()['Score']
    data = data.merge(df, how = 'inner', on = 'Country')
    return data.rename(columns={"Score_x": "Score", "Score_y": "Country_median_score"})




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

def list_of_dict_to_buffer(data: Union[List]):
    if isinstance(List):
        csv = pd.DataFrame(data=data).to_csv()
        return BytesIO(csv.encode())
    else:
        return data


def list_of_tuples_to_json(data: Union[list], field_names: list):
    if isinstance(data, list):
        list_of_dicts = [dict(zip(field_names, tup)) for tup in data]
        d_out = BytesIO()
        for d in list_of_dicts:
            d_out.write(json.dumps(d).encode())
        d_out.seek(0)
        return d_out
    else:
        return data