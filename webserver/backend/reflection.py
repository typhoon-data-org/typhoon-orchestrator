import os
import pkgutil
import re
import sys
from importlib import import_module, util
from inspect import getmembers, isfunction, getmodule


def get_modules_in_package(package_name: str):
    package = import_module(package_name)
    return get_modules_in_package_path(package.__path__)


def get_modules_in_package_path(package_path: str):
    modules = pkgutil.iter_modules(package_path)
    return [module.name for module in modules]


def get_functions_in_module(module_name: str):
    module = import_module(module_name)
    return getmembers(module, lambda func: isfunction(func) and getmodule(func) == module)


def get_function_names_in_module(module_name: str):
    return [x[0] for x in get_functions_in_module(module_name)]


def get_function_data_in_module(module_name: str):
    return [_make_function_data(x) for x in get_functions_in_module(module_name)]


class BrokenImportError(object):
    pass


def get_functions_in_module_path(module_path: str):
    module = load_module_from_path(module_path)

    return getmembers(module, lambda func: isfunction(func) and getmodule(func) is None)


def get_function_names_in_module_path(module_path: str):
    return [x[0] for x in get_functions_in_module_path(module_path)]


def _get_type(x):
    if not x:
        return None

    matches = re.match(r"<class '([^']+)'>", str(x))
    if matches:
        type_str = matches.group(1)
    else:
        type_str = str(x)
    return re.sub(r'~(\w+)', r'<\1>', type_str)


def _make_function_data(x):
    return {
        'name': x[0],
        'docstring': x[1].__doc__,
        'return_type': _get_type(x[1].__annotations__.get('return')),
        'args': [{'name': k, 'type': _get_type(v)} for k, v in x[1].__annotations__.items() if k != 'return']
    }


def get_function_data_in_module_path(module_path: str):
    return [_make_function_data(x) for x in get_functions_in_module_path(module_path)]


def load_module_from_path(module_path, module_name=None, must_exist=True):
    sys.path.append(os.path.dirname(os.path.dirname(module_path)))
    if module_name is None:
        parts = module_path.split('/')
        module_name = parts[-2] + '.' + parts[-1].strip('.py')
    spec = util.spec_from_file_location(module_name, module_path)
    module = util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (NameError, SyntaxError, FileNotFoundError):
        if must_exist:
            raise BrokenImportError
        else:
            print(f'Module {module_name} at path {module_path} does not exist')
            return None
    return module


def package_tree(package_name: str):
    tree = {}
    for module_name in get_modules_in_package(package_name):
        full_module_name = f'{package_name}.{module_name}'
        tree[module_name] = {}
        for function_data in get_function_data_in_module(full_module_name):
            tree[module_name][function_data['name']] = function_data

    return tree


def user_defined_modules(path):
    return [x[:-3] for x in os.listdir(path) if x.endswith('.py') and not x.startswith('_')]


def package_tree_from_path(package_path: str):
    tree = {}
    for file_name in user_defined_modules(package_path):
        module_name = file_name
        tree[module_name] = {}
        for function_data in get_function_data_in_module_path(os.path.join(package_path, file_name + '.py')):
            if not function_data['name'].startswith('_'):
                tree[module_name][function_data['name']] = function_data

    return tree


if __name__ == '__main__':
    fc_functions = get_function_data_in_module('typhoon.contrib.functions.flow_control')
    _modules = get_modules_in_package('typhoon.contrib.functions')
    fc_functions = get_function_names_in_module('typhoon.contrib.functions.flow_control')
    cfc_functions = get_function_names_in_module_path('/Users/biellls/Desktop/typhoon-example/functions/scrape.py')
    _tree = package_tree('typhoon.contrib.functions')
    _tree2 = package_tree_from_path('/Users/biellls/Desktop/typhoon-example/functions/')
    a = 2
