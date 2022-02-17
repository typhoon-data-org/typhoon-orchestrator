import os
from pathlib import Path
from shutil import rmtree, copytree
from typing import Union, Optional

from typhoon.core.settings import Settings


def write_to_out(filename: str, data: Union[bytes, str], directory: Optional[str] = None):
    if directory:
        path = Settings.out_directory / directory / filename
        os.makedirs(str(Settings.out_directory / directory), exist_ok=True)
    else:
        path = Settings.out_directory / filename
        os.makedirs(str(Settings.out_directory), exist_ok=True)

    print(f'Writing file to {path}')
    if isinstance(data, str):
        data = data.encode()
    with open(path, 'wb') as f:
        f.write(data)


def clean_out():
    print('Cleaning out directory...')
    rmtree(str(Settings.out_directory), ignore_errors=True)


def deploy_dag_requirements(dag: dict, local_typhoon: bool, typhoon_version: str):
    requirements = dag.get('requirements', [])
    if local_typhoon:
        from typhoon.deployment.packaging import local_typhoon_path
        typhoon_requirement = f'{local_typhoon_path()}'.rstrip('typhoon')
    else:
        typhoon_requirement = 'typhoon-orchestrator' if typhoon_version == 'latest' else f'typhoon-orchestrator={typhoon_version}'
    requirements.append(typhoon_requirement)
    if requirements:
        write_to_out(directory=dag['name'], filename='requirements.txt', data='\n'.join(requirements))


def copy_local_typhoon(dag: dict, local_typhoon_path: str):
    dag_dir = Settings.out_directory / dag['name'] / 'typhoon'
    copytree(local_typhoon_path, str(dag_dir))


def copy_user_defined_code(dag, symlink=False):
    dag_name = dag['name']
    try:
        if symlink:
            os.symlink(str(Settings.functions_directory), str(Settings.out_directory / dag_name / 'functions'))
        else:
            copytree(str(Settings.functions_directory), str(Settings.out_directory / dag_name / 'functions'))
    except FileNotFoundError:
        print('No user defined functions. Skipping copy...')
    try:
        if symlink:
            os.symlink(str(Settings.transformations_directory), str(Settings.out_directory / dag_name / 'transformations'))
        else:
            copytree(str(Settings.transformations_directory), str(Settings.out_directory / dag_name / 'transformations'))
    except FileNotFoundError:
        print('No user defined transformations. Skipping copy...')
    try:
        if symlink:
            os.symlink(str(Settings.hooks_directory), str(Settings.out_directory / dag_name / 'hooks'))
        else:
            copytree(str(Settings.hooks_directory), str(Settings.out_directory / dag_name / 'hooks'))
    except FileNotFoundError:
        print('No user defined hooks. Skipping copy...')
