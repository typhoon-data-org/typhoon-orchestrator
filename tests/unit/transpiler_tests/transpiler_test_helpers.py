from pathlib import Path
from typing import List, Tuple, Union

import yaml

from typhoon.core.dags import add_yaml_constructors


def load_transpiler_test_cases(test_module: str) -> List[Tuple[dict, Union[dict, str]]]:
    path = Path(__file__).parent / test_module / 'test_cases'
    test_cases = []
    add_yaml_constructors()
    for f in path.glob('*.yml'):
        test_case = yaml.load(f.read_text(), yaml.FullLoader)
        test_cases.append((test_case['input'], test_case['expects']))
    return test_cases
