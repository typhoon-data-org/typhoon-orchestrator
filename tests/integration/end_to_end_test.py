import subprocess
from pathlib import Path

from typing import Union

env = {
    'LC_ALL': 'C.UTF-8',
    'LANG': 'C.UTF-8'
}


def run_cli(cmd: str, cwd: Union[Path, str] = None, env: dict = None):
    proc = subprocess.run(cmd.split(' '), cwd=cwd, env=env)
    if proc.returncode != 0:
        raise Exception(f'Command {cmd} returned non zero code {proc.returncode}')
    return proc


def test_end_to_end(tmpdir):
    tmpdir = Path(tmpdir)
    typhoon = '/usr/local/bin/typhoon'
    project_home = Path('/opt/typhoon_project')
    data_lake = tmpdir/'lake'
    data_lake.mkdir()
    (project_home/'connections.yml').write_text(f'''
data_lake:
  local:
    conn_type: local_storage
    extra:
      base_path: {data_lake}
''')
    var_a_content = r'Hello_World'
    run_cli(f'{typhoon} connection add --conn-id data_lake --conn-env local', project_home, env)
    run_cli(f'{typhoon} variable add --var-id a --var-type string --contents {var_a_content}', project_home, env)
    execution_date = '2021-04-15'
    run_cli(f'{typhoon} dag run --dag-name simple --execution-date {execution_date}', project_home, env)
    assert (data_lake/execution_date.replace('-', '')/'a.txt').read_text() == var_a_content
