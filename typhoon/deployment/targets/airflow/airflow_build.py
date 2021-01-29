import re
from pathlib import Path
from shutil import rmtree
from typing import Optional

from typhoon.core.glue import load_dags
from typhoon.core.settings import Settings
from typhoon.deployment.targets.airflow.airflow_templates import AirflowDag


def build_all_dags_airflow(remote: Optional[str], matching: Optional[str] = None):
    target_folder: Path = Settings.typhoon_home.parent/'dags/typhoon_managed/'
    rmtree(str(target_folder), ignore_errors=True)
    target_folder.mkdir()

    print('Build all DAGs...')
    dags = load_dags(ignore_errors=True)
    for dag, dag_code in dags:
        print(f'Found DAG {dag.name}')
        if not matching or re.match(matching, dag.name):
            compiled_dag = AirflowDag(dag).render()
            (target_folder / f'{dag.name}.py').write_text(compiled_dag)
