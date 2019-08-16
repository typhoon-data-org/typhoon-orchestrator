from functools import lru_cache
from typing import NamedTuple, List, Union, Dict

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
        return self._asdict()


class Edge(NamedTuple):
    name: str
    source: str     # Node id
    adapter: Dict[str, Item]
    destination: str     # Node id

    def as_dict(self):
        return self._asdict()


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
