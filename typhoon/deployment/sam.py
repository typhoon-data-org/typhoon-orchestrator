from pathlib import Path
from typing import Sequence, Optional, Union

import jinja2
import pkg_resources

from typhoon.core.dags import DAGDefinitionV2
from typhoon.core.settings import Settings
from typhoon.deployment.deploy import write_to_out

SEARCH_PATH = Path(pkg_resources.resource_filename('typhoon', 'deployment')) / 'templates'
templateLoader = jinja2.FileSystemLoader(searchpath=str(SEARCH_PATH))
templateEnv = jinja2.Environment(loader=templateLoader)

templateEnv.trim_blocks = True
templateEnv.lstrip_blocks = True
templateEnv.keep_trailing_newline = True


def to_camelcase(s: str):
    return ''.join([x.capitalize() for x in s.split('_')])


templateEnv.filters.update(to_camelcase=to_camelcase)


def deploy_sam_template(dags: Sequence[Union[dict, DAGDefinitionV2]], remote: Optional[str] = None):
    sam_template = generate_sam_template(
        dags=[dag.dict() if isinstance(dag, DAGDefinitionV2) else dag for dag in dags],
        lambda_function_timeout=10*60,
        connections_table_name=Settings.connections_table_name,
        variables_table_name=Settings.variables_table_name,
        target_env=remote,
    )
    write_to_out('template.yml', sam_template)


def write_sam_template(
        dags: Sequence[dict],
        default_iam_role: str,
        lambda_function_timeout: int,
):
    sam_template = generate_sam_template(dags, default_iam_role, lambda_function_timeout)
    write_to_out('template.yml', sam_template)


def generate_sam_template(
        dags: Sequence[dict],
        lambda_function_timeout: int,
        connections_table_name: str,
        variables_table_name: str,
        target_env: str,
):
    sam_template = templateEnv.get_template('sam_template.yml.j2')
    return sam_template.render({
        'dags': dags,
        'lambda_function_timeout': lambda_function_timeout,
        'connections_table_name': connections_table_name,
        'variables_table_name': variables_table_name,
        'environment': target_env,
    })
