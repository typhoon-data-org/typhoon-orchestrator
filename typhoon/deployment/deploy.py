import os
from shutil import rmtree, copytree
from typing import Union, Sequence, Optional

from deployment.dags import generate_dag_code
from deployment.settings import out_directory, functions_directory, transformations_directory
from deployment.zappa import generate_zappa_settings


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


def build_zappa_settings(
        dags: Sequence[dict],
        aws_profile: str,
        project_name: str,
        s3_bucket: Optional[str] = None,
):
    zappa_settings = generate_zappa_settings(dags, aws_profile, project_name, s3_bucket)
    write_to_out(f'zappa_settings.json', zappa_settings)


def copy_user_defined_code():
    copytree(functions_directory(), os.path.join(out_directory(), 'functions'))
    copytree(transformations_directory(), os.path.join(out_directory(), 'transformations'))
