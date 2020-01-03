import re
from types import SimpleNamespace
from typing import List, Any

from reflection import load_module_from_path
from typhoon.core.settings import typhoon_home


def _replace_special_vars(string: str):
    string = re.sub(r'\$(\d+)', r'results[\1 - 1]', string)
    string = re.sub(r'\$BATCH', 'source_data', string)
    string = re.sub(r'\$DAG_CONTEXT\.([\w]+)', r'dag_context["""\1"""]', string)
    string = string.replace('$DAG_CONTEXT', 'dag_context')
    string = string.replace('$BATCH_NUM', '2')
    string = re.sub(r'\$VARIABLE(\.(\w+))', r'TyphoonConfig().metadata_store.get_variable("\g<2>").get_contents()', string)
    return string


# noinspection PyUnresolvedReferences
def run_transformations(source_data: Any, dag_context: dict, user_transformations: List[str]):
    import os
    import typhoon.contrib.transformations as typhoon
    from typhoon.core.config import TyphoonConfig

    custom_transformation_modules = {}
    transformations_path = os.path.join(typhoon_home(), 'transformations')
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

    os.environ['TYPHOON_ENV'] = 'dev'

    results = []
    for transform in map(_replace_special_vars, user_transformations):
        try:
            custom_locals = locals()
            custom_locals['transformations'] = custom_transformations
            result = eval(transform, globals(), custom_locals)
        except Exception as e:
            result = {'__error__': f'{type(e).__name__}: {str(e)}'}
        results.append(result)

    return results[-1]