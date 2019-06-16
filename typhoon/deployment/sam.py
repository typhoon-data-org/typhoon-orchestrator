from pathlib import Path
from typing import Sequence, Optional

import jinja2

from typhoon.core import get_typhoon_config
from typhoon.deployment.deploy import write_to_out

SEARCH_PATH = Path(__file__).parent / 'templates'
templateLoader = jinja2.FileSystemLoader(searchpath=str(SEARCH_PATH))
templateEnv = jinja2.Environment(loader=templateLoader)

templateEnv.trim_blocks = True
templateEnv.lstrip_blocks = True
templateEnv.keep_trailing_newline = True


def to_camelcase(s: str):
    return ''.join([x.capitalize() for x in s.split('_')])


templateEnv.filters.update(to_camelcase=to_camelcase)


def deploy_sam_template(dags: Sequence[dict], use_cli_config: bool = False, target_env: Optional[str] = None):
    config = get_typhoon_config(use_cli_config, target_env)
    sam_template = generate_sam_template(
        dags=dags,
        default_iam_role=config.iam_role_name,
        lambda_function_timeout=config.lambda_function_timeout,
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
        default_iam_role: str,
        lambda_function_timeout: int,
):
    zappa_settings_template = templateEnv.get_template('sam_template.yml.j2')
    return zappa_settings_template.render({
        'dags': dags,
        'default_iam_profile': default_iam_role,
        'lambda_function_timeout': lambda_function_timeout,
    })