import os
import re
from pathlib import Path
from shutil import rmtree
from typing import Optional

from airflow import AirflowException
from airflow.bin.cli import process_subdir
from airflow.models import DagBag
from typhoon.core.glue import load_dags
from typhoon.core.settings import Settings
from typhoon.deployment.targets.airflow.airflow_database import set_airflow_db
from typhoon.deployment.targets.airflow.airflow_templates import AirflowDag
from typhoon.metadata_store_impl.airflow_metadata_store import AirflowMetadataStore


def build_all_dags_airflow(remote: Optional[str], matching: Optional[str] = None):
    airflow_home = Path(os.environ['AIRFLOW_HOME'])
    target_folder: Path = airflow_home/'dags/typhoon_managed/'
    target_folder.mkdir(parents=True, exist_ok=True)
    rmtree(str(target_folder), ignore_errors=True)
    target_folder.mkdir()

    print('Build all DAGs...')
    dags = load_dags(ignore_errors=True)
    for dag, dag_code in dags:
        print(f'Found DAG {dag.name}')
        if not matching or re.match(matching, dag.name):
            store: AirflowMetadataStore = Settings.metadata_store()
            start_date = None
            with set_airflow_db(store.db_path, store.fernet_key) as db:
                dag_run = db.get_first_dag_run(dag.name)
                if dag_run:
                    start_date = dag_run.execution_date.replace(tzinfo=None)
            compiled_dag = AirflowDag(dag, start_date=start_date).render()
            (target_folder / f'{dag.name}.py').write_text(compiled_dag)
