import os
from shutil import rmtree, copytree, copy
from typing import Union, Sequence, Optional

from typhoon.deployment.dags import generate_dag_code
from typhoon.settings import out_directory, functions_directory, transformations_directory, \
    adapters_directory, typhoon_directory
from typhoon.deployment.zappa import generate_zappa_settings


def write_to_out(filename: str, data: Union[bytes, str]):
    os.makedirs(out_directory(), exist_ok=True)

    mode = 'wb' if isinstance(data, bytes) else 'w'
    with open(os.path.join(out_directory(), filename), mode) as f:
        f.write(data)


def clean_out():
    rmtree(out_directory(), ignore_errors=True)


def build_dag_code(dag: dict, env: str):
    dag_code = generate_dag_code(dag, env)
    write_to_out(f'{dag["name"]}.py', dag_code)


def build_zappa_settings(
        dags: Sequence[dict],
        aws_profile: str,
        project_name: str,
        s3_bucket: str,
):
    zappa_settings = generate_zappa_settings(dags, aws_profile, project_name, s3_bucket)
    write_to_out(f'zappa_settings.json', zappa_settings)


def copy_user_defined_code():
    copytree(functions_directory(), os.path.join(out_directory(), 'functions'))
    copytree(adapters_directory(), os.path.join(out_directory(), 'adapters'))
    copytree(transformations_directory(), os.path.join(out_directory(), 'transformations'))
    copy(os.path.join(typhoon_directory(), 'typhoonconfig.cfg'), os.path.join(out_directory(), 'typhoonconfig.cfg'))
