"""Contains code that stitches together different parts of the library. By containing most side effects here the rest
of the code can be more deterministic and testable.
This code should not be unit tested.
"""
from pathlib import Path
from typing import Union, List

import yaml

from typhoon.core.dags import DAG
from typhoon.core.transpiler import transpile


def transpile_dag_and_store(dag: dict, output_path: Union[str, Path], env: str, debug_mode: bool = False):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dag_code = transpile(dag, env, debug_mode)
    Path(output_path).write_text(dag_code)


def load_dags(dags_directory: Union[str, Path]) -> List[DAG]:
    dags_directory = Path(dags_directory)
    dags = []

    for dag_file in dags_directory.rglob('*.yml'):
        dag = yaml.safe_load(dag_file.read_text())
        dags.append(DAG.from_dict_definition(dag))

    return dags
