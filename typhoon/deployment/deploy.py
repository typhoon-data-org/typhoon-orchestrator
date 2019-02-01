import os
from shutil import rmtree
from typing import Union, Sequence

from deployment.dags import generate_dag_code
from deployment.settings import typhoon_directory, out_directory


def write_to_out(filename: str, data: Union[bytes, str]):
    os.makedirs(out_directory(), exist_ok=True)

    mode = 'wb' if isinstance(data, bytes) else 'w'
    with open(os.path.join(out_directory(), filename), mode) as f:
        f.write(data)


def clean_out():
    rmtree(out_directory())


def build_dag_code(dag: dict):
    dag_code = generate_dag_code(dag)
    write_to_out(f'{dag["name"]}.py', dag_code)
