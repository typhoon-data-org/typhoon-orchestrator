import inspect
import pkgutil
from importlib import import_module
from pathlib import Path
from pkgutil import ModuleInfo
from types import ModuleType
from typing import List, Tuple, Dict, Type

from typing_extensions import TypedDict

from typhoon.contrib.hooks.hook_interface import HookInterface

ExtensionInfo = Tuple[ModuleInfo, ModuleType]
ExtensionsList = List[ExtensionInfo]


def get_typhoon_extensions() -> ExtensionsList:
    extensions = []
    for module_info in pkgutil.iter_modules():
        if not module_info.name.startswith('typhoon_'):
            continue
        try:
            module = __import__(module_info.name)
            if getattr(module, '__typhoon_extension__', False):
                extensions.append((module_info, module))
        except ImportError:
            print(f'Error importing {module_info.name}')
    return extensions


class ExtensionsInfo(TypedDict):
    """Key: Module name (eg: relational), Value: Full module path (eg: typhoon_dbapi.relational)"""
    functions: Dict[str, str]
    transformations: Dict[str, str]
    hooks: Dict[str, str]
    components: Dict[str, str]


def get_typhoon_extensions_info(extensions: ExtensionsList = None) -> ExtensionsInfo:
    if extensions is None:
        extensions = get_typhoon_extensions()
    info = dict(functions={}, transformations={}, hooks={}, components={})
    for module_info, module in extensions:
        module_path = Path(module.__file__).parent
        for ext_folder in ['functions', 'transformations', 'hooks']:
            for sub_module_path in (module_path/ext_folder).rglob('*.py'):
                function_module_name = sub_module_path.stem
                if function_module_name.startswith('__'):
                    continue
                info[ext_folder][function_module_name] = str(sub_module_path.relative_to(module_path.parent)) \
                    .replace('.py', '').replace('/', '.')
            for component_path in (module_path/'components').rglob('*.yml'):
                component_name = component_path.stem
                info['components'][component_name] = str(component_path)
    typhoon_contrib_path = Path(__file__).parent.parent / 'contrib'
    for ext_folder in ['functions', 'transformations', 'hooks']:
        for sub_module_path in (typhoon_contrib_path / ext_folder).rglob('*.py'):
            function_module_name = sub_module_path.stem
            if function_module_name.startswith('__'):
                continue
            info[ext_folder][function_module_name] = str(sub_module_path.relative_to(typhoon_contrib_path.parent.parent)) \
                .replace('.py', '').replace('/', '.')
    for component_path in (typhoon_contrib_path / 'components').rglob('*.yml'):
        component_name = component_path.stem
        info['components'][component_name] = str(component_path)
    return info


def get_hooks_info(extensions_info: ExtensionsInfo = None) -> Dict[str, Type[HookInterface]]:
    hooks_info = {}
    if extensions_info is None:
        extensions_info = get_typhoon_extensions_info()
    for module_name, import_from in extensions_info['hooks'].items():
        module = import_module(import_from)
        for cls_name, cls in inspect.getmembers(module, inspect.isclass):
            conn_type = getattr(cls, 'conn_type', None)
            if conn_type:
                hooks_info[conn_type] = cls
    return hooks_info
