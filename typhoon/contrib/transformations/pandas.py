from io import StringIO
from pathlib import Path
from typing import Union

import pandas as pd


def from_csv(csv: Union[bytes, str, StringIO, Path], delimiter: str = ',') -> pd.DataFrame:
    if isinstance(csv, Path):
        print(f'Reading csv from path {csv}...')
        csv = csv.read_text()
    elif isinstance(csv, bytes):
        print(f'Decoding csv from bytes...')
        csv = csv.decode()
    if isinstance(csv, str):
        print(f'Converting csv string into StringIO...')
        csv = StringIO(csv)
    return pd.read_csv(csv, delimiter=delimiter)
