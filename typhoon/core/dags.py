import hashlib
from datetime import datetime
from typing import NamedTuple, List, Union, Dict

import dateutil.parser
from dataclasses import dataclass

Item = Union[int, str, float, Dict, List]


class Node(NamedTuple):
    name: str
    function: str
    asynchronous: bool = True
    config: Item = None

    def __post_init__(self):
        if not self.config:
            self.config = {}

    def as_dict(self):
        node = self._asdict()
        node.pop('name')
        return node


class Edge(NamedTuple):
    name: str
    source: str     # Node id
    adapter: Dict[str, Item]
    destination: str     # Node id

    def as_dict(self):
        edge = self._asdict()
        edge.pop('name')
        return edge


class DAG(NamedTuple):
    name: str
    schedule_interval: str
    nodes: Dict[str, Node]
    edges: Dict[str, Edge]
    active: bool = True

    def __post_init__(self):
        # Validate schedule interval
        pass

    def as_dict(self) -> Dict:
        dag = self._asdict()
        dag['nodes'] = {k: v.as_dict() for k, v in self.nodes.items()}
        dag['edges'] = {k: v.as_dict() for k, v in self.edges.items()}
        return dag

    @staticmethod
    def from_dict_definition(dag: Dict):
        kwargs = dag.copy()
        kwargs['nodes'] = {name: Node(name=name, **node) for name, node in dag['nodes'].items()}
        kwargs['edges'] = {name: Edge(name=name, **edge) for name, edge in dag['edges'].items()}
        return DAG(**kwargs)

    @property
    # @lru_cache(1)
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
    # @lru_cache(1)
    def non_source_nodes(self) -> List[str]:
        """All nodes that are the destination of an edge"""
        return [node for x in self.structure.values() for node in x]

    @property
    # @lru_cache(1)
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


class DagContext(NamedTuple):
    execution_date: datetime
    ds: str
    ds_nodash: str
    ts: str
    etl_timestamp: str

    @staticmethod
    def from_date_string(date_string) -> 'DagContext':
        execution_date = dateutil.parser.parse(date_string)
        return DagContext(
            execution_date=execution_date,
            ds=execution_date.strftime('%Y-%m-%d'),
            ds_nodash=execution_date.strftime('%Y-%m-%d').replace('-', ''),
            ts=execution_date.strftime('%Y-%m-%dT%H:%M:%S'),
            etl_timestamp=datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        )

    def to_dict(self) -> Dict:
        d = self._asdict()
        d['execution_date'] = str(d['execution_date'])
        return d

    @staticmethod
    def from_dict(d: Dict) -> 'DagContext':
        return DagContext.from_date_string(d['execution_date'])


def hash_dag_code(dag_code: str) -> str:
    m = hashlib.sha1()
    m.update(dag_code.encode())
    return m.hexdigest()


@dataclass
class DagDeployment:
    dag_name: str
    deployment_date: datetime
    dag_code: str

    @property
    def deployment_hash(self) -> str:
        return hash_dag_code(self.dag_code)

    def to_dict(self) -> dict:
        return dict(deployment_hash=self.deployment_hash, **self.__dict__)

    @staticmethod
    def from_dict(d: dict) -> 'DagDeployment':
        return DagDeployment(
            dag_name=d['dag_name'],
            deployment_date=dateutil.parser.parse(d['deployment_date']),
            dag_code=d['dag_code'],
        )
