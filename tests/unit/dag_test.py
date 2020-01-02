import pytest

from typhoon.core.dags import DAG

simple_dag_without_cycles = {
    'name': 'test_dag',
    'schedule_interval': '@daily',
    'nodes': {
        'a': {
            'function': 'foo'
        },
        'b': {
            'function': 'bar'
        },
        'c': {
            'function': 'baz'
        }
    },
    'edges': {
        'e1': {
            'source': 'a',
            'adapter': None,
            'destination': 'c'
        },
        'e2': {
            'source': 'b',
            'adapter': None,
            'destination': 'c'
        },
    }
}

simple_dag_with_cycles = {
    'name': 'test_dag',
    'schedule_interval': '@daily',
    'nodes': {
        'a': {
            'function': 'foo'
        },
        'b': {
            'function': 'bar'
        },
        'c': {
            'function': 'baz'
        }
    },
    'edges': {
        'e1': {
            'source': 'a',
            'adapter': None,
            'destination': 'b'
        },
        'e2': {
            'source': 'b',
            'adapter': None,
            'destination': 'c'
        },
        'e3': {
            'source': 'c',
            'adapter': None,
            'destination': 'a'
        }
    }
}


@pytest.mark.parametrize('dag_dict,has_cycle', [(simple_dag_without_cycles, False), (simple_dag_with_cycles, True)])
def test_has_cycle(dag_dict, has_cycle):
    dag = DAG.parse_obj(dag_dict)
    assert dag.has_cycle == has_cycle
