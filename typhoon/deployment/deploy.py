import os
from pathlib import Path
from shutil import rmtree, copytree, copy
from typing import Union, Sequence, Optional

from typhoon.deployment.dags import generate_dag_code
from typhoon.deployment.zappa import generate_zappa_settings
from typhoon.core.settings import out_directory, functions_directory, transformations_directory, \
    typhoon_home, hooks_directory


def write_to_out(filename: str, data: Union[bytes, str], directory: Optional[str] = None):
    if directory:
        path = Path(out_directory()) / directory / filename
        os.makedirs(os.path.join(out_directory(), directory), exist_ok=True)
    else:
        path = Path(out_directory()) / filename
        os.makedirs(out_directory(), exist_ok=True)

    print(f'Writing file to {path}')
    if isinstance(data, str):
        data = data.encode()
    with open(path, 'wb') as f:
        f.write(data)


def clean_out():
    rmtree(out_directory(), ignore_errors=True)


def build_dag_code(dag: dict, env: str, debug_mode: bool = False):
    dag_code = generate_dag_code(dag, env, debug_mode)
    dag_name = dag['name']
    write_to_out(directory=dag_name, filename=f'{dag_name}.py', data=dag_code)


def typhoon_requirements():
    requirements_path: Path = Path(__file__).parent.parent.parent / 'requirements.txt'
    return [x for x in requirements_path.read_text().splitlines() if 'boto' not in x]


def deploy_dag_requirements(dag: dict, local_typhoon: bool, typhoon_version: str):
    requirements = dag.get('requirements', [])
    if local_typhoon:
        requirements = list(set(requirements).union(typhoon_requirements()))
    else:
        typhoon_requirement = 'typhoon' if typhoon_version == 'latest' else f'typhoon={typhoon_version}'
        requirements.append(typhoon_requirement)
    if requirements:
        write_to_out(directory=dag['name'], filename='requirements.txt', data='\n'.join(requirements))


def copy_local_typhoon(dag: dict, local_typhoon_path: str):
    dag_dir = Path(out_directory(), dag['name'], 'typhoon')
    copytree(local_typhoon_path, dag_dir)


def build_zappa_settings(
        dags: Sequence[dict],
        aws_profile: str,
        project_name: str,
        s3_bucket: str,
        env: str,
):
    zappa_settings = generate_zappa_settings(dags, aws_profile, project_name, s3_bucket, env)
    write_to_out(f'zappa_settings.json', zappa_settings)


def old_copy_user_defined_code():
    copytree(functions_directory(), os.path.join(out_directory(), 'functions'))
    copytree(transformations_directory(), os.path.join(out_directory(), 'transformations'))
    copytree(hooks_directory(), os.path.join(out_directory(), 'hooks'))
    copy(os.path.join(typhoon_home(), 'typhoonconfig.cfg'), os.path.join(out_directory(), 'typhoonconfig.cfg'))


def copy_user_defined_code(dag):
    dag_name = dag['name']
    try:
        copytree(functions_directory(), os.path.join(out_directory(), dag_name, 'functions'))
    except FileNotFoundError:
        print('No user defined functions. Skipping copy...')
    try:
        copytree(transformations_directory(), os.path.join(out_directory(), dag_name, 'transformations'))
    except FileNotFoundError:
        print('No user defined transformations. Skipping copy...')
    try:
        copytree(hooks_directory(), os.path.join(out_directory(), dag_name, 'hooks'))
    except FileNotFoundError:
        print('No user defined hooks. Skipping copy...')
    copy(os.path.join(typhoon_home(), 'typhoonconfig.cfg'), os.path.join(out_directory(), dag_name, 'typhoonconfig.cfg'))
