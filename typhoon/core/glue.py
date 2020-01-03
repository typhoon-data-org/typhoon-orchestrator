"""Contains code that stitches together different parts of the library. By containing most side effects here the rest
of the code can be more deterministic and testable.
This code should not be unit tested.
"""
import os
from pathlib import Path
from typing import Union, List

import yaml

from typhoon.core.dags import DAG
from typhoon.core.settings import Settings
from typhoon.core.transpiler import transpile


def transpile_dag_and_store(dag: dict, output_path: Union[str, Path], env: str, debug_mode: bool = False):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dag_code = transpile(dag, env, debug_mode)
    Path(output_path).write_text(dag_code)


def load_dags() -> List[DAG]:
    dags = []
    for dag_file in Settings.dags_directory.rglob('*.yml'):
        dag = yaml.safe_load(dag_file.read_text())
        dags.append(DAG.parse_obj(dag))

    return dags


def get_dags_contents(dags_directory: Union[str, Path]) -> List[str]:
    dags_directory = Path(dags_directory)

    dags = []
    for dag_file in dags_directory.rglob('*.yml'):
        dags.append(dag_file.read_text())

    return dags


def get_dag_filenames():
    dag_files = filter(lambda x: x.endswith('.yml'), os.listdir(str(Settings.dags_directory)))
    return dag_files
