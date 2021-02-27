from pathlib import Path

import yaml

from typhoon.core.dags import add_yaml_constructors, DAGDefinitionV2
from typhoon.core.settings import Settings

if __name__ == '__main__':
    Settings.typhoon_home = '/Users/biellls/Desktop/typhoon_airflow/typhoon_extension/'
    dag_file = Path('/Users/biellls/Desktop/typhoon_airflow/typhoon_extension/dags/test_if.yml')
    add_yaml_constructors()
    dd = DAGDefinitionV2(**yaml.load(dag_file.read_text(), yaml.FullLoader))
    dag = dd.make_dag()
