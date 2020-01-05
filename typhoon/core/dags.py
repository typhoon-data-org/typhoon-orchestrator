import hashlib
import re
from datetime import datetime
from enum import Enum
from typing import List, Union, Dict, Any

from pydantic import BaseModel, validator, Field, root_validator

Identifier = Field(..., regex=r'\w+')
Item = Union[int, str, float, Dict, List]


class SpecialCronString(str, Enum):
    daily = '@daily'
    weekly = '@weekly'
    monthly = '@monthly'
    yearly = '@yearly'


class Node(BaseModel):
    function: str = Field(..., regex=r'(typhoon\.\w+\.\w+|functions\.\w+\.\w+)')
    asynchronous: bool = True
    config: Dict[str, Any] = Field(default={})

    @validator('config')
    def validate_config_keys(cls, v):
        for k in v.keys():
            if not re.match(r'\w+(\s*=>\s*APPLY)?', k):
                raise ValueError(f'Config key "{k}" does not match pattern. Must be identifier with optional => APPLY')
        return v


class Edge(BaseModel):
    source: str = Identifier     # Node id
    adapter: Dict[str, Item]
    destination: str = Identifier     # Node id

    @validator('adapter')
    def validate_adapter_keys(cls, v):
        for k in v.keys():
            if not re.match(r'\w+(\s*=>\s*APPLY)?', k):
                raise ValueError(f'Config key "{k}" does not match pattern. Must be identifier with optional => APPLY')
        return v


class DAG(BaseModel):
    name: str = Identifier
    schedule_interval: str = Field(
        ...,
        regex='(' + '@daily|@weekly|@monthly|@yearly|' +
              r'((\*|\?|\d+((\/|\-){0,1}(\d+))*)\s*){5,6}' + '|' +
              r'rate\(\s*1\s+minute\s*\)' + '|' +
              r'rate\(\s*\d+\s+minutes\s*\)' + ')'
    )
    nodes: Dict[str, Node]
    edges: Dict[str, Edge]
    active: bool = True

    @root_validator
    def validate_undefined_nodes_in_edges(cls, values):
        if 'nodes' not in values.keys():
            return values       # Nodes did not pass upstream validations
        node_names = values['nodes'].keys()
        for edge_name, edge in values['edges'].items():
            if edge.source not in node_names:
                raise ValueError(f'Source for edge "{edge_name}" is not defined: "{edge.source}"')
            if edge.destination not in node_names:
                raise ValueError(f'Destination for edge "{edge_name}" is not defined: "{edge.destination}"')
        return values

    @property
    def structure(self) -> Dict[str, List[str]]:
        """For every node all the nodes it's connected to"""
        structure = {}
        for _, edge in self.edges.items():
            if edge.source not in structure.keys():
                structure[edge.source] = [edge.destination]
            else:
                structure[edge.source].append(edge.destination)

        return structure

    @property
    def non_source_nodes(self) -> List[str]:
        """All nodes that are the destination of an edge"""
        return [node for x in self.structure.values() for node in x]

    @property
    def sources(self) -> List[str]:
        """Nodes that are sources of the DAG"""
        sources = set(self.structure.keys())
        destinations = set(self.non_source_nodes)
        return list(sources.difference(destinations))

    def get_edge(self, source: str, destination: str) -> Edge:
        for edge_name, edge in self.edges.items():
            if edge.source == source and edge.destination == destination:
                return edge
        assert False

    def get_edges_for_source(self, source) -> List[Edge]:
        return [edge for edge in self.edges.values() if edge.source == source]

    def out_nodes(self, source: str) -> List[str]:
        return self.structure.get(source, ())

    @property
    def has_cycle(self):
        white, gray, black = 0, 1, 2
        color = {n: white for n in self.nodes.keys()}

        def dfs(node):
            color[node] = gray
            for dest in self.structure.get(node, []):
                if color[dest] == gray:
                    return True
                elif color[dest] == white and dfs(dest):
                    return True
            color[node] = black
            return False

        for n in self.nodes:
            if color[n] == white and dfs(n):
                return True
        return False


class DagContext(BaseModel):
    execution_date: datetime
    etl_timestamp: datetime = Field(default=datetime.now())

    @property
    def ds(self) -> str:
        return self.execution_date.strftime('%Y-%m-%d')

    @property
    def ds_nodash(self) -> str:
        return self.execution_date.strftime('%Y%m%d')

    @property
    def ts(self) -> str:
        return self.execution_date.strftime('%Y-%m-%dT%H:%M:%S')


def hash_dag_code(dag_code: str) -> str:
    m = hashlib.sha1()
    m.update(dag_code.encode())
    return m.hexdigest()


class DagDeployment(BaseModel):
    dag_name: str
    deployment_date: datetime
    dag_code: str

    @property
    def deployment_hash(self) -> str:
        return hash_dag_code(self.dag_code)
