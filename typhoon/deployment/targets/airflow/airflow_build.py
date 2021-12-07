import os
import re
from pathlib import Path
from shutil import rmtree
from typing import Optional

from typhoon.core.components import Component
from typhoon.core.glue import load_dag_definitions, load_components
from typhoon.core.settings import Settings
from typhoon.core.transpiler.component_transpiler import ComponentFile
from typhoon.core.transpiler.task_transpiler import TasksFile
from typhoon.deployment.targets.airflow.airflow_database import set_airflow_db
from typhoon.deployment.targets.airflow.dag_transpiler import AirflowDagFile
from typhoon.metadata_store_impl.airflow_metadata_store import AirflowMetadataStore


def build_all_dags_airflow(remote: Optional[str], matching: Optional[str] = None):
    airflow_home = Path(os.environ['AIRFLOW_HOME'])
    target_folder: Path = airflow_home/'dags/typhoon_managed/'
    target_folder.mkdir(parents=True, exist_ok=True)
    rmtree(str(target_folder), ignore_errors=True)
    target_folder.mkdir()

    print('Build all DAGs...')
    dags = load_dag_definitions(ignore_errors=True)
    for dag, _ in dags:
        print(f'Found DAG {dag.name}')
        if not matching or re.match(matching, dag.name):
            dag_target_folder = target_folder / dag.name
            dag_target_folder.mkdir(parents=True, exist_ok=True)
            init_path = (dag_target_folder / '__init__.py')
            init_path.write_text('')

            store: AirflowMetadataStore = Settings.metadata_store()
            start_date = None
            with set_airflow_db(store.db_path, store.fernet_key) as db:
                dag_run = db.get_first_dag_run(dag.name)
                if dag_run:
                    start_date = dag_run.execution_date.replace(tzinfo=None)

            compiled_dag = AirflowDagFile(dag, start_date=start_date, debug_mode=remote is None).render()
            (dag_target_folder / f'{dag.name}.py').write_text(compiled_dag)
            tasks_code = TasksFile(dag.tasks).render()
            (dag_target_folder / 'tasks.py').write_text(tasks_code)

    for component, _ in load_components(ignore_errors=False, kind='all'):
        print(f'Building component {component.name}...')
        build_component_for_airflow(component, target_folder)


def build_component_for_airflow(component: Component, target_folder: Path):
    components_folder_path = target_folder / 'components'
    components_folder_path.mkdir(parents=True, exist_ok=True)
    init_path = (components_folder_path / '__init__.py')
    if not init_path.exists():
        init_path.write_text('')
    component_code = ComponentFile(component).render()
    (components_folder_path / f'{component.name}.py').write_text(component_code)
