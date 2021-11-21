import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from distutils.dir_util import copy_tree
from distutils.errors import DistutilsFileError
from pathlib import Path
from shutil import copytree, copy, make_archive, move, rmtree
from typing import Optional, List

import pkg_resources

from typhoon.core.components import Component
from typhoon.core.dags import DagDeployment, DAGDefinitionV2
from typhoon.core.glue import transpile_dag_and_store, load_dag_definitions, load_components
from typhoon.core.settings import Settings
from typhoon.core.transpiler.component_transpiler import ComponentFile
from typhoon.deployment.deploy import deploy_dag_requirements, copy_local_typhoon, copy_user_defined_code


def package_dag(
        dag_name: str,
        venv_path: str,
):
    with tempfile.TemporaryDirectory(prefix=dag_name) as temp_dir:
        _copy_venv_site_packages(temp_dir, venv_path)
        # add_handler(temp_dir)
        try:
            copytree(str(Settings.functions_directory), os.path.join(temp_dir, 'functions'))
        except FileNotFoundError:
            print('No functions directory')
        try:
            copytree(str(Settings.transformations_directory), os.path.join(temp_dir, 'transformations'))
        except FileNotFoundError:
            print('No transformations directory')
        try:
            copytree(str(Settings.hooks_directory), os.path.join(temp_dir, 'hooks'))
        except FileNotFoundError:
            print('No hooks directory')
        try:
            copy(os.path.join(str(Settings.typhoon_home), 'typhoonconfig.cfg'), temp_dir)
        except FileNotFoundError:
            print('No typhoonconfig.cfg')
        copy(
            src=os.path.join(str(Settings.out_directory), f'{dag_name}.py'),
            dst=temp_dir,
        )

        make_archive(
            base_name=dag_name,
            format='zip',
            root_dir=temp_dir,
        )
        move(f'{dag_name}.zip', os.path.join(str(Settings.out_directory)))


def _copy_venv_site_packages(temp_dir: str, venv_path: str):
    copy_tree(
        src=_venv_site_packages_path(venv_path),
        dst=temp_dir,
        # symlinks=False,
        # ignore=ignore_patterns('*boto*')
    )
    try:
        copy_tree(
            src=_venv_site_packages_path(venv_path, bin64=True),
            dst=temp_dir,
            # symlinks=False,
            # ignore=ignore_patterns('*boto*')
        )
    except DistutilsFileError:
        pass

    # Deletes boto related packages (included in lambda)
    for name in os.listdir(temp_dir):
        if 'boto' in name:
            rmtree(os.path.join(temp_dir, name))


def _python_version() -> str:
    return 'python{}.{}'.format(*sys.version_info)


def _venv_site_packages_path(venv_path: str, bin64=False) -> str:
    lib = 'lib64' if bin64 else 'lib'
    return os.path.join(venv_path, lib, _python_version(), 'site-packages')


def get_current_venv():
    """
    Returns the path to the current virtualenv
    """
    if 'VIRTUAL_ENV' in os.environ:
        venv = os.environ['VIRTUAL_ENV']
    elif os.path.exists('.python-version'):  # pragma: no cover
        try:
            subprocess.check_output(['pyenv', 'help'], stderr=subprocess.STDOUT)
        except OSError:
            print("This directory seems to have pyenv's local venv, "
                  "but pyenv executable was not found.")
        with open('.python-version', 'r') as f:
            # minor fix in how .python-version is read
            # Related: https://github.com/Miserlou/Zappa/issues/921
            env_name = f.readline().strip()
        bin_path = subprocess.check_output(['pyenv', 'which', 'python']).decode('utf-8')
        venv = bin_path[:bin_path.rfind(env_name)] + env_name
    else:  # pragma: no cover
        return None
    return venv


def build_all_dags(remote: Optional[str], matching: Optional[str] = None) -> List[Path]:
    from typhoon.deployment.deploy import clean_out
    from typhoon.deployment.sam import deploy_sam_template

    clean_out()

    print('Build all DAGs...')
    deployment_date = datetime.now()
    dags = load_dag_definitions(ignore_errors=True)
    deploy_sam_template([dag for dag, _ in dags], remote=remote)
    dag_files = []
    for dag, dag_file in dags:
        if not matching or re.match(matching, dag.name):
            dag_files.append(dag_file)
            build_dag(dag, dag_file, deployment_date, remote)

            used_components = set()
            for node in dag.tasks.values():
                if node.component is not None:
                    used_components.add(node.component.split('.')[-1])
            for component, _ in load_components(ignore_errors=False):
                if component.name in used_components:
                    print(f'Building component {component.name}...')
                    build_component(dag.name, component, remote)

    print('Finished building DAGs\n')
    return dag_files


def build_component(dag_name: str, component: Component, remote: Optional[str]):
    components_folder_path = Settings.out_directory / dag_name / 'components'
    components_folder_path.mkdir(parents=True, exist_ok=True)
    init_path = (components_folder_path / '__init__.py')
    if not init_path.exists():
        init_path.write_text('')
    component_code = ComponentFile(component).render()
    (components_folder_path / f'{component.name}.py').write_text(component_code)


def build_dag(dag: DAGDefinitionV2, dag_file: Path, deployment_date: datetime, remote: Optional[str]):
    dag = dag.dict()
    dag_folder = Settings.out_directory / dag['name']
    transpile_dag_and_store(dag, dag_folder, debug_mode=remote is None)
    deploy_dag_requirements(dag, typhoon_version_is_local(), Settings.typhoon_version)
    if typhoon_version_is_local():
        print('Typhoon package is in editable mode. Copying to lambda package...')
        copy_local_typhoon(dag, local_typhoon_path())
    if not remote:
        print('Setting up user defined code as symlink for debugging...')
    copy_user_defined_code(dag, symlink=remote is None)
    if remote is None:
        Settings.metadata_store(aws_profile=None).set_dag_deployment(
            DagDeployment(
                dag_name=dag['name'],
                deployment_date=deployment_date,
                dag_code=dag_file.read_text(),
            )
        )


def dist_is_editable(dist) -> bool:
    """Is distribution an editable install?"""
    for path_item in sys.path:
        egg_link = os.path.join(path_item, dist.project_name + '.egg-link')
        if os.path.isfile(egg_link):
            return True
    return False


def typhoon_version_is_local() -> bool:
    typhoon_dist = [dist for dist in pkg_resources.working_set if dist.key == 'typhoon-orchestrator'][0]
    return dist_is_editable(typhoon_dist)


def local_typhoon_path() -> str:
    import typhoon
    import inspect
    return str(Path(inspect.getfile(typhoon)).parent)
