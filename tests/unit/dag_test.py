import pytest
import yaml

from typhoon.core.dags import DAG

simple_dag_without_cycles = yaml.safe_load("""
name: test_dag
schedule_interval: "@daily"

nodes:
    a:
        function: typhoon.example.foo
    b:
        function: typhoon.example.bar
    c:
        function: typhoon.example.baz

edges:
    e1:
        source: a
        adapter: {}
        destination: c
    e2:
        source: b
        adapter: {}
        destination: c
""")

simple_dag_with_cycles = yaml.safe_load("""
name: test_dag
schedule_interval: "@daily"

nodes:
    a:
        function: typhoon.example.foo
    b:
        function: typhoon.example.bar
    c:
        function: typhoon.example.baz

edges:
    e1:
        source: a
        adapter: {}
        destination: c
    e2:
        source: b
        adapter: {}
        destination: c
    e3:
        source: c
        adapter: {}
        destination: a
""")


@pytest.mark.parametrize('dag_dict,has_cycle', [(simple_dag_without_cycles, False), (simple_dag_with_cycles, True)])
def test_has_cycle(dag_dict, has_cycle):
    dag = DAG.parse_obj(dag_dict)
    assert dag.has_cycle == has_cycle
