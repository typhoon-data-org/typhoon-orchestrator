import os
from typing import Sequence, Tuple, Iterable

import jinja2 as jinja2
import yaml

from typhoon.deployment import settings


def load_dags() -> Sequence:
    dags_directory = settings.dags_directory()
    dags = []

    dag_files = filter(lambda x: x.endswith('.yml'), os.listdir(dags_directory))
    for dag_file in dag_files:
        with open(os.path.join(dags_directory, dag_file), 'r') as f:
            dag = yaml.load(f)
            dag['structure'] = build_dag_structure(dag['edges'])
            dags.append(dag)

    return dags


def build_dag_structure(edges: dict) -> dict:
    structure = {}
    for _, edge in edges.items():
        if edge['source'] not in structure.keys():
            structure[edge['source']] = [edge['destination']]
        else:
            structure[edge['source']].append(edge['destination'])

    return structure


def get_sources(structure: dict) -> Sequence[str]:
    sources = set(structure.keys())
    destinations = set(get_destinations(structure))
    return list(sources.difference(destinations))


def get_sinks(structure: dict) -> Sequence[str]:
    sources = set(structure.keys())
    destinations = set(get_destinations(structure))
    return list(destinations.difference(sources))


def get_transformations(edges: dict, source: str, destination: str) -> Sequence[str]:
    for _, edge in edges.items():
        if edge['source'] == source and edge['destination'] == destination:
            return edge['transformations']


def get_edge(edges: dict, source: str, destination: str) -> Tuple[str, str]:
    for edge_name, edge in edges.items():
        if edge['source'] == source and edge['destination'] == destination:
            return edge_name, edge


def get_edges_for_source(edges, source) -> Tuple[str, str]:
    for edge_name, edge in edges.items():
        if edge['source'] == source:
            yield edge_name, edge


def get_destinations(structure) -> Iterable[str]:
    for x in structure.values():
        for edge in x:
            yield edge


def get_transformations_modules(edges: dict)  -> Iterable[str]:
    modules = set()
    for _, edge in edges.items():
        for transformation in edge['transformations']:
            modules.add('.'.join(transformation.split('.')[:-1]))

    return list(modules)


def get_functions_modules(nodes: dict) -> Iterable[str]:
    modules = set()
    for _, node in nodes.items():
        modules.add('.'.join(node['function'].split('.')[:-1]))

    return list(modules)


SEARCH_PATH = os.path.join(
    os.path.dirname(__file__),
    'templates')
templateLoader = jinja2.FileSystemLoader(searchpath=SEARCH_PATH)
templateEnv = jinja2.Environment(loader=templateLoader)

templateEnv.globals.update(get_sources=get_sources)
templateEnv.globals.update(get_sinks=get_sinks)
templateEnv.globals.update(get_destinations=get_destinations)
templateEnv.globals.update(get_transformations=get_transformations)
templateEnv.globals.update(get_transformations_modules=get_transformations_modules)
templateEnv.globals.update(get_functions_modules=get_functions_modules)
templateEnv.globals.update(get_edge=get_edge)
templateEnv.globals.update(get_edges_for_source=get_edges_for_source)


def generate_dag_code(dag: dict):
    dag_template = templateEnv.get_template('dag_code.py.j2')
    return dag_template.render(dag)

