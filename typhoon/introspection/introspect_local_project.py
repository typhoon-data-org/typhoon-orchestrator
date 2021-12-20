import importlib.util
import inspect
from typing import List, Any

from typhoon.core.settings import Settings
from typhoon.introspection.introspect_extensions import FunctionInfo


def local_functions_info() -> List[FunctionInfo]:
    functions_info = []
    for functions_module_path in (Settings.typhoon_home/'functions').rglob('*py'):
        spec = importlib.util.spec_from_file_location(functions_module_path.stem, functions_module_path)
        functions_module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(functions_module)
        except ModuleNotFoundError as e:
            print(f'WARNING: Error while loading module {functions_module_path}. Will not build JSON schema hints for it. {e}')
            continue
        for function_name, function in inspect.getmembers(functions_module, inspect.isfunction):
            signature = inspect.signature(function)
            functions_info.append(FunctionInfo(
                module=f'functions.{functions_module_path.stem}',
                name=function_name,
                args=signature.parameters,
                docstring=function.__doc__,
            ))
    return functions_info
