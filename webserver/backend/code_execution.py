import re
from typing import List, Any


def _replace_special_vars(string: str):
    string = re.sub(r'\$(\d+)', r'results[\1 - 1]', string)
    string = re.sub(r'\$SOURCE', 'source_data', string)
    string = re.sub(r'\$DAG_CONFIG\.([\w]+)', r'dag_config["""\1"""]', string)
    # string = string.replace('typhoon.', 'typhoon_transformations')
    return string


def run_transformations(source_data: Any, dag_config: dict, transformations: List[str]):
    import typhoon.contrib.transformations as typhoon
    results = []
    for transform in map(_replace_special_vars, transformations):
        results.append(eval(transform, globals(), locals()))

    return results[-1]
