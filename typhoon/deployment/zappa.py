import os
from typing import Sequence

import jinja2

SEARCH_PATH = os.path.join(
    os.path.dirname(__file__),
    'templates')
templateLoader = jinja2.FileSystemLoader(searchpath=SEARCH_PATH)
templateEnv = jinja2.Environment(loader=templateLoader)


def dag_details(dags):
    for dag in dags:
        yield f'{dag["name"]}.{dag["name"]}_main', dag['schedule_interval']


def generate_zappa_settings(
        dags: Sequence[dict],
        aws_profile: str,
        project_name: str,
        s3_bucket: str,
        env: str,
):
    zappa_settings_template = templateEnv.get_template('zappa_settings.json.j2')
    return zappa_settings_template.render({
        'aws_profile': aws_profile,
        'project_name': project_name,
        's3_bucket': s3_bucket,
        'dags': dag_details(dags),
        'environment': env,
    })
