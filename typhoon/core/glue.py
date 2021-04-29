"""Contains code that stitches together different parts of the library. By containing most side effects here the rest
of the code can be more deterministic and testable.
This code should not be unit tested.
"""
import os
from pathlib import Path
from typing import Union, List, Tuple, Dict, Optional

import yaml
from pydantic import ValidationError
from typhoon.core.components import Component

from typhoon.core.dags import DAG, DAGDefinitionV2, add_yaml_constructors
from typhoon.core.settings import Settings
from typhoon.core.transpiler import TyphoonFileTemplate
from typing_extensions import Literal

from typhoon.introspection.introspect_extensions import get_typhoon_extensions_info


def transpile_dag_and_store(dag: dict, output_path: Union[str, Path], debug_mode: bool):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dag_code = TyphoonFileTemplate(DAG.parse_obj(dag), debug_mode=debug_mode).render()
    Path(output_path).write_text(dag_code)


def load_dags(ignore_errors: bool = False) -> List[Tuple[DAG, str]]:
    return [(dd.make_dag(), ds) for dd, ds in load_dag_definitions(ignore_errors)]


def load_dag_definitions(ignore_errors: bool = False) -> List[Tuple[DAGDefinitionV2, str]]:
    add_yaml_constructors()
    dags = []
    for dag_file in Settings.dags_directory.rglob('*.yml'):
        if ignore_errors:
            try:
                dag = DAGDefinitionV2.parse_obj(
                    yaml.load(dag_file.read_text(), yaml.FullLoader)
                )
            except ValidationError:
                continue
        else:
            dag = DAGDefinitionV2.parse_obj(
                yaml.safe_load(dag_file.read_text())
            )
        dags.append((dag, dag_file.read_text()))

    return dags


def load_dag(dag_name: str, ignore_errors: bool = False) -> Optional[DAG]:
    dags = load_dags(ignore_errors)
    matching_dags = [(dag, code) for dag, code in dags if dag.name == dag_name]
    assert len(matching_dags) <= 1, f'Found {len(matching_dags)} dags with name "{dag_name}"'
    return matching_dags[0] if len(matching_dags) == 1 else None


def load_dag_definition(dag_name: str, ignore_errors: bool = False) -> Optional[DAGDefinitionV2]:
    dags = load_dag_definitions(ignore_errors)
    matching_dags = [(dag, code) for dag, code in dags if dag.name == dag_name]
    assert len(matching_dags) <= 1, f'Found {len(matching_dags)} dags with name "{dag_name}"'
    return matching_dags[0] if len(matching_dags) == 1 else None


def get_dag_errors() -> Dict[str, List[dict]]:
    add_yaml_constructors()
    result = {}
    for dag_file in Settings.dags_directory.rglob('*.yml'):
        try:
            DAGDefinitionV2.parse_obj(
                yaml.load(dag_file.read_text(), yaml.FullLoader)
            ).make_dag()
        except ValidationError as e:
            result[dag_file.name.split('.yml')[0]] = e.errors()

    return result


def get_dags_contents(dags_directory: Union[str, Path]) -> List[str]:
    dags_directory = Path(dags_directory)

    dags = []
    for dag_file in dags_directory.rglob('*.yml'):
        dags.append(dag_file.read_text())

    return dags


def get_dag_filenames():
    dag_files = filter(lambda x: x.endswith('.yml'), os.listdir(str(Settings.dags_directory)))
    return dag_files


def load_component(
        component_name: str,
        ignore_errors: bool = False,
        kind: Union[Literal['typhoon'], Literal['custom'], Literal['all']] = 'all',
) -> Optional[Component]:
    components = load_components(ignore_errors, kind)
    matching_components = [(component, code) for component, code in components if component.name == component_name]
    assert len(matching_components) <= 1, f'Found {len(matching_components)} dags with name "{component_name}"'
    return matching_components[0][0] if len(matching_components) == 1 else None


def load_components(
        ignore_errors: bool = False,
        kind: Union[Literal['typhoon'], Literal['custom'], Literal['all']] = 'all',
) -> List[Tuple[Component, str]]:
    if kind == 'all':
        return [(c, cs) for c, cs in load_component_definitions(ignore_errors, kind='typhoon')] + \
               [(c, cs) for c, cs in load_component_definitions(ignore_errors, kind='custom')]
    else:
        return [(c, cs) for c, cs in load_component_definitions(ignore_errors, kind)]


def load_component_definitions(
        ignore_errors,
        kind=Union[Literal['typhoon'], Literal['custom']],
) -> List[Tuple[Component, str]]:
    add_yaml_constructors()
    if kind == 'custom':
        component_files = list(Settings.components_directory.rglob('*.yml'))
    else:
        component_files = [Path(x) for x in get_typhoon_extensions_info()['components'].values()]
    components = []
    for component_file in component_files:
        if ignore_errors:
            try:
                comp = Component.parse_obj(yaml.load(component_file.read_text(), yaml.FullLoader))
            except ValidationError:
                continue
        else:
            comp = Component.parse_obj(yaml.load(component_file.read_text(), yaml.FullLoader))
        components.append((comp, component_file.read_text()))

    return components
