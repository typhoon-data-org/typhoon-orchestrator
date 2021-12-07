import json
import logging
import os
import pydoc
import shutil
import subprocess
import sys
from builtins import AssertionError
from datetime import datetime
from multiprocessing import Process
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import click
import jinja2
import pkg_resources
import pygments
from dataclasses import asdict
from datadiff import diff
from datadiff.tools import assert_equal
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.data import YamlLexer
from pygments.lexers.python import PythonLexer
from streamlit import bootstrap
from tabulate import tabulate
from termcolor import colored

from api.main import run_api
from typhoon import connections
from typhoon.cli_helpers.cli_completion import get_remote_names, get_dag_names, get_conn_envs, get_conn_ids, \
    get_var_types, get_deploy_targets, PROJECT_TEMPLATES, get_task_names
from typhoon.cli_helpers.json_schema import generate_json_schemas
from typhoon.cli_helpers.status import dags_with_changes, dags_without_deploy, check_connections_yaml, \
    check_connections_dags, check_variables_dags
from typhoon.connections import Connection
from typhoon.contrib.hooks.hook_factory import get_hook
from typhoon.core import DagContext
from typhoon.core.dags import ArgEvaluationError, load_module_from_path
from typhoon.core.glue import get_dag_errors, load_dag_definition
from typhoon.core.settings import Settings
from typhoon.deployment.packaging import build_all_dags, local_typhoon_path
from typhoon.handler import run_dag
from typhoon.introspection.introspect_extensions import get_typhoon_extensions, get_typhoon_extensions_info, \
    get_hooks_info
from typhoon.introspection.introspect_transformations import TransformationResult
from typhoon.local_config import EXAMPLE_CONFIG
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore
from typhoon.remotes import Remotes
from typhoon.variables import Variable, VariableType
from typhoon.watch import watch_changes

ascii_art_logo = r"""
 _________  __  __   ______   ___   ___   ______   ______   ___   __        
/________/\/_/\/_/\ /_____/\ /__/\ /__/\ /_____/\ /_____/\ /__/\ /__/\      
\__.::.__\/\ \ \ \ \\:::_ \ \\::\ \\  \ \\:::_ \ \\:::_ \ \\::\_\\  \ \     
   \::\ \   \:\_\ \ \\:(_) \ \\::\/_\ .\ \\:\ \ \ \\:\ \ \ \\:. `-\  \ \    
    \::\ \   \::::_\/ \: ___\/ \:: ___::\ \\:\ \ \ \\:\ \ \ \\:. _    \ \   
     \::\ \    \::\ \  \ \ \    \: \ \\::\ \\:\_\ \ \\:\_\ \ \\. \`-\  \ \  
      \__\/     \__\/   \_\/     \__\/ \::\/ \_____\/ \_____\/ \__\/ \__\/  
"""


def set_settings_from_remote(remote: str):
    if remote:
        if remote not in Remotes.remotes_config.keys():
            print(f'Remote {remote} is not defined in .typhoonremotes. Found : {list(Remotes.remotes_config.keys())}',
                  file=sys.stderr)
            sys.exit(-1)
        Settings.metadata_db_url = Remotes.metadata_db_url(remote)
        Settings.fernet_key = Remotes.fernet_key(remote)
        if Remotes.use_name_as_suffix(remote):
            Settings.metadata_suffix = remote


@click.group()
def cli():
    """Typhoon CLI"""
    logging.getLogger('sqlitedict').setLevel(logging.CRITICAL)


@cli.command()
@click.argument('project_name')
@click.option('--deploy-target', autocompletion=get_deploy_targets, required=False, default='typhoon',
              help='Target for DAG deployment. It can be "typhoon" or "airflow"')
@click.option('--template', required=False, default='hello_world',
              help=f'Project template. One of {PROJECT_TEMPLATES}')
def init(project_name: str, deploy_target: str, template: str):
    """Create a new Typhoon project"""
    example_project_path = Path(pkg_resources.resource_filename('typhoon', 'examples')) / template
    dest = Path.cwd() / project_name
    shutil.copytree(str(example_project_path), str(dest))

    if template == 'extension':
        module_path = dest / 'module'
        module_path.rename(dest / project_name)
        setup_path = dest / 'setup.py'
        setup_path.write_text(
            data=jinja2.Template(setup_path.read_text()).render(name=project_name))
        print(f'Extension created in {dest}')
    else:
        if template == 'airflow_docker':
            cfg_path = dest / 'src/typhoon.cfg'
            dag_schema_path = dest / 'src/dag_schema.json'
            component_schema_path = dest / 'src/component_schema.json'
        else:
            cfg_path = (dest / 'typhoon.cfg')
            dag_schema_path = (dest / 'dag_schema.json')
            component_schema_path = (dest / 'component_schema.json')

        cfg_path.write_text(EXAMPLE_CONFIG.format(project_name=project_name, deploy_target=deploy_target))
        Settings.typhoon_home = dest
        dag_schema, component_schema = generate_json_schemas()
        dag_json_schema = json.dumps(dag_schema, indent=2)
        dag_schema_path.write_text(dag_json_schema)
        component_json_schema = json.dumps(component_schema, indent=2)
        component_schema_path.write_text(component_json_schema)

        print(f'Project created in {dest}')
        print('If you want auto completion run the following:')
        for shell_type in ['bash', 'zsh', 'fish']:
            print(f'In {shell_type}', colored(f'eval "$(_TYPHOON_COMPLETE=source_{shell_type} typhoon)"', 'blue'))


@cli.command()
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def status(remote: Optional[str]):
    """Information on project status"""
    set_settings_from_remote(remote)

    print(colored(ascii_art_logo, 'cyan'))
    if not Settings.typhoon_home:
        print(colored(f'FATAL: typhoon home not found...', 'red'))
        return
    else:
        print(colored('• Typhoon home defined as', 'green'), colored(Settings.typhoon_home, 'blue'))

    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if metadata_store.exists():
        print(colored('• Metadata database found in', 'green'), colored(Settings.metadata_db_url, 'blue'))
        check_connections_yaml(remote)
        check_connections_dags(remote)
        check_variables_dags(remote)
    elif isinstance(metadata_store, SQLiteMetadataStore):
        print(colored('• Metadata store not found for', 'yellow'), colored(Settings.metadata_db_url, 'blue'))
        print(
            colored('   - It will be created upon use, or create by running (idempotent) command', color=None),
            colored(f'typhoon migrate{" " + remote if remote else ""}', 'blue'))
        print(colored('  Skipping connections and variables checks...', 'red'))
    else:
        print(colored('• Metadata store not found or incomplete for', 'red'), colored(Settings.metadata_db_url, 'blue'))
        print(
            colored('   - Fix by running (idempotent) command', color=None),
            colored(f'typhoon metadata migrate{" " + remote if remote else ""}', 'blue'))
        print(colored('  Skipping connections and variables checks...', 'red'))

    if not remote:
        changed_dags = dags_with_changes()
        if changed_dags:
            print(colored('• Unbuilt changes in DAGs... To rebuild run', 'yellow'),
                  colored(f'typhoon dag build{" " + remote if remote else ""} --all [--debug]', 'blue'))
            for dag in changed_dags:
                print(colored(f'   - {dag}', 'blue'))
        else:
            print(colored('• DAGs up to date', 'green'))
    else:
        undeployed_dags = dags_without_deploy(remote)
        if undeployed_dags:
            print(colored('• Undeployed changes in DAGs... To deploy run', 'yellow'),
                  colored(f'typhoon dag push {remote} --all [--build-dependencies]', 'blue'))
            for dag in undeployed_dags:
                print(colored(f'   - {dag}', 'blue'))
        else:
            print(colored('• DAGs up to date', 'green'))


@cli.group(name='remote')
def cli_remote():
    """Manage Typhoon remotes"""
    pass


@cli_remote.command(name='add')
@click.argument('remote')       # No autocomplete because the remote is new
@click.option('--aws-profile')
@click.option('--metadata-db-url')
@click.option('--use-name-as-suffix', is_flag=True, default=False)
def remote_add(remote: str, aws_profile: str, metadata_db_url: str, use_name_as_suffix: bool):
    """Add a remote for deployments and management"""
    Remotes.add_remote(remote, aws_profile, metadata_db_url, use_name_as_suffix)
    print(f'Added remote {remote}')


@cli_remote.command(name='ls')
@click.option('-l', '--long', is_flag=True, default=False)
def remote_list(long: bool):
    """List configured Typhoon remotes"""
    if long:
        header = ['REMOTE_NAME', 'AWS_PROFILE', 'USE_NAME_AS_SUFFIX', 'METADATA_DB_URL']
        table_body = [
            [remote, Remotes.aws_profile(remote), Remotes.use_name_as_suffix(remote), Remotes.metadata_db_url(remote)]
            for remote in Remotes.remote_names
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for remote in Remotes.remote_names:
            print(remote)


@cli_remote.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names)       # No autocomplete because the remote is new
def remote_add(remote: str):
    """Remove remote"""
    Remotes.remove_remote(remote)
    print(f'Removed remote {remote}')


@cli.group(name='metadata')
def cli_metadata():
    """Manage Typhoon metadata"""
    pass


@cli_metadata.command()
@click.argument('remote', autocompletion=get_remote_names)
def migrate(remote: str):
    """Create the necessary metadata tables"""
    set_settings_from_remote(remote)
    print(f'Migrating {Settings.metadata_db_url}...')
    Settings.metadata_store(aws_profile=Remotes.aws_profile(remote)).migrate()


@cli_metadata.command(name='info')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def metadata_info(remote: Optional[str]):
    """Info on metadata connection and table names"""
    set_settings_from_remote(remote)
    print(ascii_art_logo)
    print(f'Metadata database URL:\t{Settings.metadata_db_url}')
    print(f'Connections table name:\t{Settings.connections_table_name}')
    print(f'Variables table name:\t{Settings.variables_table_name}')
    print(f'DAG deployments table name:\t{Settings.dag_deployments_table_name}')


@cli.group(name='dag')
def cli_dags():
    """Manage Typhoon DAGs"""
    pass


@cli_dags.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_dags(remote: Optional[str], long: bool):
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if long:
        header = ['DAG_NAME', 'DEPLOYMENT_DATE']
        table_body = [
            [x.dag_name, x.deployment_date.isoformat()]
            for x in metadata_store.get_dag_deployments()
        ]
        print(tabulate(table_body, header, 'plain'))
        if not remote:
            dag_errors = get_dag_errors()
            if dag_errors:
                header = ['DAG_NAME', 'ERROR LOCATION', 'ERROR MESSAGE']
                table_body = [
                    [dag_name, error[0]['loc'], error[0]['msg']]
                    for dag_name, error in dag_errors.items()
                ]
                print(colored(tabulate(table_body, header, 'plain'), 'red'), file=sys.stderr)
    else:
        for dag_name in sorted(set(x.dag_name for x in metadata_store.get_dag_deployments())):
            print(dag_name)
        if not remote:
            for dag_name, _ in get_dag_errors().items():
                print(colored(dag_name, 'red'), file=sys.stderr)


@cli_dags.command(name='build')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--dag-name', autocompletion=get_dag_names, required=False, default=None)
@click.option('--all', 'all_', is_flag=True, default=False, help='Build all DAGs (mutually exclusive with DAG_NAME)')
def build_dags(remote: Optional[str], dag_name: Optional[str], all_: bool):
    """Build code for dags in $TYPHOON_HOME/out/"""
    set_settings_from_remote(remote)
    if dag_name and all_:
        raise click.UsageError(f'Illegal usage: DAG_NAME is mutually exclusive with --all')
    elif dag_name is None and not all_:
        raise click.UsageError(f'Illegal usage: Need either DAG_NAME or --all')
    if all_:
        dag_errors = get_dag_errors()
        if dag_errors:
            print(f'Found errors in the following DAGs:')
            for dag_name in dag_errors.keys():
                print(f'  - {dag_name}\trun typhoon dag build {dag_name}')
            sys.exit(-1)

        if Settings.deploy_target == 'typhoon':
            build_all_dags(remote=remote)
        else:
            try:
                from typhoon.deployment.targets.airflow.airflow_build import build_all_dags_airflow
            except ModuleNotFoundError:
                print('ERROR: Airflow is not installed. Try "pip install apache-airflow~=1.10"')
                sys.exit(-1)
            try:
                os.environ['AIRFLOW_HOME']
            except KeyError:
                print('Error: AIRFLOW_HOME is not set')
                sys.exit(-1)
            build_all_dags_airflow(remote=remote)
    else:
        dag_errors = get_dag_errors().get(dag_name)
        if dag_errors:
            print(f'FATAL: DAG {dag_name} has errors', file=sys.stderr)
            header = ['ERROR_LOCATION', 'ERROR_MESSAGE']
            table_body = [
                [error['loc'], error['msg']]
                for error in dag_errors
            ]
            print(tabulate(table_body, header, 'plain'), file=sys.stderr)
            sys.exit(-1)
        if Settings.deploy_target == 'typhoon':
            build_all_dags(remote=None, matching=dag_name)
        else:
            from typhoon.deployment.targets.airflow.airflow_build import build_all_dags_airflow
            build_all_dags_airflow(remote=None, matching=dag_name)


@cli_dags.command(name='watch')
@click.argument('dag_name', autocompletion=get_dag_names, required=False, default=None)
@click.option('--all', 'all_', is_flag=True, default=False, help='Build all DAGs (mutually exclusive with DAG_NAME)')
def watch_dags(dag_name: Optional[str], all_: bool):
    """Watch DAGs and build code for dags in $TYPHOON_HOME/out/"""
    if dag_name and all_:
        raise click.UsageError(f'Illegal usage: DAG_NAME is mutually exclusive with --all')
    elif dag_name is None and not all_:
        raise click.UsageError(f'Illegal usage: Need either DAG_NAME or --all')
    if all_:
        print('Watching all DAGs for changes...')
        watch_changes()
    else:
        print(f'Watching DAG {dag_name} for changes...')
        watch_changes(patterns=f'{dag_name}.yml')


def run_local_dag(dag_name: str, execution_date: datetime):
    run_dag(dag_name, str(execution_date), capture_logs=False)


@cli_dags.command(name='run')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--dag-name', autocompletion=get_dag_names)
@click.option('--execution-date', default=None, type=click.DateTime(), help='DAG execution date as YYYY-mm-dd')
def cli_run_dag(remote: Optional[str], dag_name: str, execution_date: Optional[datetime]):
    """Run a DAG for a specific date. Will create a metadata entry in the database (TODO: create entry)."""
    set_settings_from_remote(remote)
    if execution_date is None:
        execution_date = datetime.now()
    if remote is None:
        print(f'Running {dag_name} from local build...')
        dag_errors = get_dag_errors().get(dag_name)
        if dag_errors:
            print(f'FATAL: DAG {dag_name} has errors', file=sys.stderr)
            header = ['ERROR_LOCATION', 'ERROR_MESSAGE']
            table_body = [
                [error['loc'], error['msg']]
                for error in dag_errors
            ]
            print(tabulate(table_body, header, 'plain'), file=sys.stderr)
            sys.exit(-1)

        build_all_dags(remote=None, matching=dag_name)
        run_local_dag(dag_name, execution_date)
    else:
        # TODO: Run lambda function
        pass


@cli_dags.command(name='definition')
@click.option('--dag-name', autocompletion=get_dag_names)
def dag_definition(dag_name: str):
    """Show definition of DAG"""
    matching_dags = list(Settings.dags_directory.rglob(f'*{dag_name}.yml'))
    if not matching_dags:
        print(f'FATAL: No DAGs found matching {dag_name}.yml', file=sys.stderr)
        sys.exit(-1)
    elif len(matching_dags) > 1:
        print(f'FATAL: Expected one matching DAG for {dag_name}.yml. Found {len(matching_dags)}', file=sys.stderr)
    out = colored(ascii_art_logo, 'cyan') + '\n' + pygments.highlight(
        code=matching_dags[0].read_text(),
        lexer=YamlLexer(),
        formatter=Terminal256Formatter()
    )
    pydoc.pager(out)
    print(matching_dags[0])


@cli_dags.group(name='task')
def cli_tasks():
    """Manage Typhoon DAG tasks"""
    pass


def eval_batch(batch: str):
    custom_locals = {}
    custom_locals['PyObj'] = SimpleNamespace
    try:
        import pandas as pd
        custom_locals['pd'] = pd
    except ImportError:
        print('Warning: could not import pandas. Run pip install pandas if you want to use dataframes')
    try:
        batch = eval(batch, {}, custom_locals)
        return batch
    except Exception as e:
        print(f'FATAL: Got an error while trying to evaluate input: "{e}"', file=sys.stderr)
        sys.exit(-1)


@cli_tasks.command(name='sample')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--dag-name', autocompletion=get_dag_names, required=True)
@click.option('--task-name', autocompletion=get_task_names, required=True)
@click.option('--batch', help='Input batch to task transformations', required=True)
@click.option('--batch-num', help='Batch number', type=int, required=False, default=1)
@click.option('--interval-start', type=click.DateTime(), default=None)
@click.option('--interval-end', type=click.DateTime(), default=None)
@click.option('--eval', 'eval_', is_flag=True, default=False, help='If true evaluate the input string')
def task_sample(
        remote: Optional[str],
        dag_name: str,
        task_name: str,
        batch,
        batch_num,
        interval_start: datetime,
        interval_end: datetime,
        eval_: bool,
):
    """Test transformations for task"""
    set_settings_from_remote(remote)
    dag = load_dag_definition(dag_name)
    if task_name not in dag.tasks.keys():
        print(f'FATAL: No tasks found matching the name "{task_name}" in dag {dag_name}', file=sys.stderr)
        sys.exit(-1)
    if eval_:
        batch = eval_batch(batch)
    interval_start = interval_start or datetime.now()
    if interval_start and interval_end:
        dag_context = DagContext(
            schedule_interval=dag.schedule_interval,
            interval_start=interval_start,
            interval_end=interval_end,
            granularity=dag.granularity,
        )
    else:
        dag_context = DagContext.from_cron_and_event_time(
            schedule_interval=dag.schedule_interval,
            event_time=datetime.now(),
            granularity=dag.granularity,
        )
    transformation_results = dag.tasks[task_name].execute_adapter(
        batch,
        dag_context,
        batch_num,
    )
    for config_item, result in transformation_results.items():
        if isinstance(result, ArgEvaluationError):
            # traceback.print_exception(result.error_type, result.error_value, result.traceback, limit=5, file=sys.stderr)
            print(f'{config_item}: {result.error_type.__name__} {result.error_value}', file=sys.stderr)
        else:
            highlighted_result = pygments.highlight(
                code=TransformationResult(config_item, result).pretty_result,
                lexer=PythonLexer(),
                formatter=Terminal256Formatter()
            )
            print(colored(f'{config_item}:', 'green'), highlighted_result, end='')


@cli_dags.command(name='test')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--dag-name', autocompletion=get_dag_names, required=True)
def dag_test(remote: Optional[str], dag_name: str):
    """Run DAG YAML tests"""
    set_settings_from_remote(remote)
    dag = load_dag_definition(dag_name)
    if dag.tests is None:
        print(f'No tests found for DAG {dag_name}')
        return
    passed = 0
    failed = 0
    errors = 0
    for task_name, test_case in dag.tests.items():
        if task_name not in dag.tasks.keys():
            print(f'Warning: No task named {task_name}. Skipping tests for it...')
            continue
        transformation_results = dag.tasks[task_name].execute_adapter(
            batch=test_case.evaluated_batch,
            dag_context=test_case.dag_context,
            batch_num=test_case.batch_num,
        )
        for arg, expected_value in test_case.evaluated_expected.items():
            result = transformation_results[arg]
            if isinstance(result, ArgEvaluationError):
                print(f'Error evaluating arg "{arg}"')
                # traceback.print_exception(result.error_type, result.error_value, result.traceback, limit=5, file=sys.stderr)
                print(f'Error evaluating arg "{arg}". {result.error_type.__name__}: {result.error_value}')
                failed += 1
                errors += 1
                continue
            failed_message = f'Failed test for "{arg}"'
            try:
                import pandas as pd
                if isinstance(expected_value, pd.DataFrame):
                    if isinstance(result, pd.DataFrame):
                        print(f'Converting DataFrames to dicts for arg "{arg}"')
                        expected_value = expected_value.to_dict()
                        result = result.to_dict()
                    else:
                        print(failed_message + f'. Expected DataFrame but got {result} of type {type(result)}')
                        failed += 1
                        continue
                elif isinstance(result, pd.DataFrame):
                    print(f'Did not expect a DataFrame as result, expected {type(expected_value)}')
                    continue
            except ImportError:
                pass

            try:
                assert_equal(expected_value, result, msg=failed_message)
                passed += 1
            except AssertionError as e:
                print(e)
                excluded_from_diff = (str, int)
                if isinstance(expected_value, excluded_from_diff) or isinstance(result, excluded_from_diff):
                    print(f'Expected "{expected_value}" but found "{result}"')
                else:
                    print(diff(expected_value, result))
                failed += 1
    if failed > 0:
        print(colored(f'{failed} tests failed', 'red'))
    print(colored(f'{passed} tests passed', 'green'))


@cli.group(name='connection')
def cli_connection():
    """Manage Typhoon connections"""
    pass


@cli_connection.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_connections(remote: Optional[str], long: bool):
    """List connections in the metadata store"""
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if long:
        header = ['CONN_ID', 'TYPE', 'HOST', 'PORT', 'SCHEMA']
        table_body = [
            [conn.conn_id, conn.conn_type, conn.host, conn.port, conn.schema]
            for conn in metadata_store.get_connections()
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for conn in metadata_store.get_connections():
            print(conn.conn_id)


@cli_connection.command(name='add')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--conn-id', autocompletion=get_conn_ids)
@click.option('--conn-env', autocompletion=get_conn_envs)
def add_connection(remote: Optional[str], conn_id: str, conn_env: str):
    """Add connection to the metadata store"""
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    conn_params = connections.get_connection_local(conn_id, conn_env)
    metadata_store.set_connection(Connection(conn_id=conn_id, **asdict(conn_params)))
    print(f'Connection {conn_id} added')


@cli_connection.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--conn-id', autocompletion=get_conn_ids, required=True)
def remove_connection(remote: Optional[str], conn_id: str):
    """Remove connection from the metadata store"""
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    metadata_store.delete_connection(conn_id)
    print(f'Connection {conn_id} deleted')


@cli_connection.command(name='definition')
def connections_definition():
    """Connection definition in connections.yml"""
    out = pygments.highlight(
        code=(Settings.typhoon_home / 'connections.yml').read_text(),
        lexer=YamlLexer(),
        formatter=Terminal256Formatter()
    )
    pydoc.pager(out)


@cli.group(name='variable')
def cli_variable():
    """Manage Typhoon variables"""
    pass


@cli_variable.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_variables(remote: Optional[str], long: bool):
    """List variables in the metadata store"""
    def var_contents(var: Variable) -> str:
        if var.type == VariableType.NUMBER:
            return var.contents
        else:
            return f'"{var.contents}"' if len(var.contents) < max_len_var else f'"{var.contents[:max_len_var]}"...'
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    if long:
        max_len_var = 40
        header = ['VAR_ID', 'TYPE', 'CONTENT']
        table_body = [
            [var.id, var.type, var_contents(var)]
            for var in metadata_store.get_variables()
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for var in metadata_store.get_variables():
            print(var.id)


@cli_variable.command(name='add')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--var-id', required=True)
@click.option('--var-type', required=True, autocompletion=get_var_types, help=f'One of {get_var_types(None, None, "")}')
@click.option('--contents', prompt=True, help='Value for the variable. Can be piped from STDIN or prompted if empty.')
def add_variable(remote: Optional[str], var_id: str, var_type: str, contents):
    """Add variable to the metadata store"""
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    var = Variable(var_id, VariableType[var_type.upper()], contents)
    metadata_store.set_variable(var)
    print(f'Variable {var_id} added')


@cli_variable.command(name='load')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--file', type=click.Path(exists=True), required=True, help='Path of variable to load')
def load_variable(remote: Optional[str], file: str):
    """Read variable from file and add it to the metadata store"""
    var = Variable.from_file(file)
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    metadata_store.set_variable(var)
    print(f'Variable {var.id} added')


@cli_variable.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('--var-id')
def remove_variable(remote: Optional[str], var_id: str):
    """Remove connection from the metadata store"""
    set_settings_from_remote(remote)
    metadata_store = Settings.metadata_store(Remotes.aws_profile(remote))
    metadata_store.delete_variable(var_id)
    print(f'Variable {var_id} deleted')


@cli.command(name='generate-json-schemas')
def cli_generate_json_schemas():
    """Generate JSON schemas using function data"""
    dag_schema, component_schema = generate_json_schemas()
    dag_json_schema = json.dumps(dag_schema, indent=2)
    (Settings.typhoon_home/'dag_schema.json').write_text(dag_json_schema)
    component_json_schema = json.dumps(component_schema, indent=2)
    (Settings.typhoon_home/'component_schema.json').write_text(component_json_schema)


@cli.group(name='extension')
def cli_extension():
    """Manage Typhoon extensions"""
    pass


@cli_extension.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def list_extensions(remote: Optional[str]):
    # set_settings_from_remote(remote)
    for extension_info, _ in get_typhoon_extensions():
        print(extension_info.name)


@cli_extension.group(name='functions')
def cli_extension_functions():
    """Manage Typhoon extension functions"""
    pass


@cli_extension_functions.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def list_extension_functions(remote: Optional[str]):
    # set_settings_from_remote(remote)
    header = ['Module', 'Path']
    table_body = []
    for module_name, module_path in get_typhoon_extensions_info()['functions'].items():
        table_body.append([module_name, module_path])
    print(tabulate(table_body, header, 'plain'))


@cli_extension.group(name='transformations')
def cli_extension_transformations():
    """Manage Typhoon extension transformations"""
    pass


@cli_extension_transformations.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def list_extension_transformations(remote: Optional[str]):
    # set_settings_from_remote(remote)
    header = ['Module', 'Path']
    table_body = []
    for module_name, module_path in get_typhoon_extensions_info()['transformations'].items():
        table_body.append([module_name, module_path])
    print(tabulate(table_body, header, 'plain'))


@cli_extension.group(name='hooks')
def cli_extension_hooks():
    """Manage Typhoon extension transformations"""
    pass


@cli_extension_hooks.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def list_extension_hooks(remote: Optional[str]):
    # set_settings_from_remote(remote)
    header = ['Hook Type', 'Class name']
    table_body = []
    extensions_info = get_typhoon_extensions_info()
    for conn_type, cls in get_hooks_info(extensions_info).items():
        table_body.append([conn_type, cls.__name__])
    print(tabulate(table_body, header, 'plain'))


@cli_extension.group(name='components')
def cli_extension_components():
    """Manage Typhoon extension transformations"""
    pass


@cli_extension_components.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
def list_extension_components(remote: Optional[str]):
    # set_settings_from_remote(remote)
    header = ['Component', 'Path']
    table_body = []
    for component_name, component_path in get_typhoon_extensions_info()['components'].items():
        table_body.append([component_name, component_path])
    print(tabulate(table_body, header, 'plain'))


class SubprocessError(Exception):
    pass


def run_in_subprocess(command: str, cwd: str):
    print(f'Executing command in shell:  {command}')
    args = command.split(' ')
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=os.environ.copy(), cwd=cwd)
    stdout, stderr = p.communicate()
    stdout = stdout.decode()
    if stderr:
        raise SubprocessError(f'Error executing in console: {stderr}')
    elif 'Unable to upload artifact' in stdout:
        print(stdout)
        raise SubprocessError(f'Error executing in console: {stdout}')
    print(stdout)


@cli.command()
def webserver():
    """Start the Component UI and API"""
    api_process = Process(target=run_api)
    api_process.start()
    ui_script = Path(local_typhoon_path()).parent/'component_ui/component_builder.py'
    try:
        bootstrap.run(str(ui_script), f'run.py {ui_script}', [], {})
    finally:
        api_process.terminate()
        api_process.join()


def transformations_locals():
    custom_transformation_modules = {}
    transformations_path = str(Settings.transformations_directory)
    for filename in os.listdir(transformations_path):
        if filename == '__init__.py' or not filename.endswith('.py'):
            continue
        module_name = filename[:-3]
        module = load_module_from_path(
            module_path=os.path.join(transformations_path, filename),
            module_name=module_name,
        )
        custom_transformation_modules[module_name] = module
    custom_transformations = SimpleNamespace(**custom_transformation_modules)
    return custom_transformations


@cli.command()
@click.option('--dag-name', autocompletion=get_dag_names, required=False, default=None)
def shell(dag_name: Optional[str]):
    """Interactive (Ipython) shell for running tasks"""
    from IPython import start_ipython
    from traitlets.config import Config
    c = Config()
    c.InteractiveShellApp.extensions = [
        'typhoon.shell_extensions'
    ]
    c.InteractiveShellApp.exec_lines = [
        'import typhoon',
    ]
    if dag_name:
        c.InteractiveShellApp.exec_lines.append(f'%load_dag {dag_name}')
    c.TerminalInteractiveShell.banner2 = f"""
{ascii_art_logo}
Pre-loaded variables:
    - tasks: Task objects generated from dag definition, ready to execute
    - dag_context: Pre-loaded DAG context to test with
    - dag: Dag definition (parsed from YAML)
Example usage:
    - tasks.example.function?
    - tasks.example.echo.get_args(dag_context, None, batch_num=1, batch='Hello world!')
    - tasks.example.echo.run(dag_context, None, batch_num=1, batch='Hello world!')
    # After changes in the YAML we can reload the DAG
    - %reload_dag
    """
    user_ns = {
        'dag_context': DagContext.from_cron_and_event_time('@daily', datetime.now(), granularity='day'),
        'transformations': transformations_locals(),
        'SimpleNamespace': SimpleNamespace,
        'get_hook': get_hook,
    }
    start_ipython(argv=[], user_ns=user_ns, config=c)

# @cli.command()
# @click.argument('remote', autocompletion=get_remote_names)
# @click.option('--build-dependencies', default=False, is_flag=True, help='Build DAG dependencies in Docker container')
# def deploy_dags(remote, build_dependencies):
#     from typhoon.core import get_typhoon_config
#
#     config = get_typhoon_config(use_cli_config=True, target_env=target_env)
#
#     build_all_dags(target_env=target_env, debug=False)
#     if build_dependencies:
#         run_in_subprocess(f'sam build --use-container', cwd=out_directory())
#     build_dir = str(Path(out_directory()) / '.aws-sam' / 'build')
#     run_in_subprocess(
#         f'sam package --template-file template.yaml --s3-bucket {config.s3_bucket} --profile {config.aws_profile}'
#         f' --output-template-file out_template.yaml',
#         cwd=build_dir
#     )
#     run_in_subprocess(
#         f'sam deploy --template-file out_template.yaml '
#         f'--stack-name {config.project_name.replace("_", "-")} --profile {config.aws_profile} '
#         f'--region {config.deploy_region} --capabilities CAPABILITY_IAM',
#         cwd=build_dir
#     )
#
#     if not config.development_mode:
#         for dag_code in get_dags_contents(settings.dags_directory()):
#             loaded_dag = yaml.safe_load(dag_code)
#             if loaded_dag.get('active', True):
#                 dag_deployment = DagDeployment(loaded_dag['name'], deployment_date=datetime.utcnow(), dag_code=dag_code)
#                 config.metadata_store.set_dag_deployment(dag_deployment)

if __name__ == '__main__':
    cli()
