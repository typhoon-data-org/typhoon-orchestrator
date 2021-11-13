import inspect
from functools import lru_cache
from inspect import Parameter
from typing import List, Type

import yaml
from typing_extensions import Literal

from typhoon.contrib.hooks.hook_interface import HookInterface
from typhoon.core.dags import DAGDefinitionV2
from typhoon.core.settings import Settings
from typhoon.introspection.introspect_extensions import get_typhoon_extensions_info, functions_info_in_module_path, \
    FunctionInfo
from typhoon.introspection.introspect_local_project import local_functions_info


def make_function_snippet(function_info: FunctionInfo) -> dict:
    return {
        'label': f'{function_info.module}.{function_info.name}',
        'body': f'{function_info.module}.{function_info.name}',
    }


def is_hook(t: Type) -> bool:
    try:
        return isinstance(t.__new__(t), HookInterface)
    except Exception:
        return False


@lru_cache(1)
def get_connection_names() -> List[str]:
    connection_definitions = yaml.safe_load((Settings.typhoon_home/'connections.yml').read_text())
    return list(connection_definitions.keys())


def is_literal(t) -> bool:
    try:
        return t.__class__
    except Exception:
        return False


def make_arg_completion(arg: Parameter) -> dict:
    completion = {
        'title': arg.name,
        # "default": [],
        # "type": "array"
    }
    if arg.annotation == str:
        completion['type'] = "string"
    elif arg.annotation == bool:
        completion['type'] = "boolean"
    elif arg.annotation == int:
        completion['type'] = "integer"
    elif arg.annotation == float:
        completion['type'] = "number"
    elif is_hook(arg.annotation):
        conn_names = get_connection_names()
        choice = '|' + ','.join(conn_names) + '|' if conn_names else ':foo'
        completion['defaultSnippets'] = [{'label': '$Hook', 'body': r'\$Hook ${1' + f'{choice}' + '}'}]
    elif arg.annotation.__class__ == Literal.__class__:
        completion['enum'] = list(arg.annotation.__values__)
    if arg.default != inspect._empty:
        completion['default'] = arg.default
    return completion


def make_function_args_completions(function_info: FunctionInfo) -> dict:
    return {
            'if': {
                'properties': {
                    'function': {
                        'const': f'{function_info.module}.{function_info.name}'
                    }
                }
            },
            'then': {
                'properties': {
                    'args': {
                        'properties': {
                            arg_name: make_arg_completion(arg)
                            for arg_name, arg in function_info.args.items()
                        }
                    }
                }
            }
        }


def generate_json_schemas() -> dict:
    """Generate schema with pydantic then customise it to add better completion for VS Code"""
    schema = DAGDefinitionV2.schema()
    extensions_info = get_typhoon_extensions_info()

    function_snippets = []
    function_args_completion = []
    for module_name, module_path in extensions_info['functions'].items():
        completion = f'typhoon.{module_name}' if module_path.startswith('typhoon') else f'functions.{module_name}'
        function_snippets.append({'label': completion, 'body': completion})
        for function_info in functions_info_in_module_path(module_name, module_path):
            function_snippets.append(make_function_snippet(function_info))
            function_args_completion.append(make_function_args_completions(function_info))
    for function_info in local_functions_info():
        function_snippets.append(make_function_snippet(function_info))
        function_args_completion.append(make_function_args_completions(function_info))
    schema['definitions']['TaskDefinition']['properties']['function']['defaultSnippets'] = function_snippets
    schema['definitions']['TaskDefinition']['allOf'] = function_args_completion
    return schema
