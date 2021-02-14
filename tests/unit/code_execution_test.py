from typhoon.core.dags import TaskDefinition, Py, MultiStep

example_task = TaskDefinition(
    function='typhoon.example.f',
    args=dict(
        a=Py('2 + $BATCH'),
        b={'a': 'aa', 'b': ['aa', Py('$BATCH + 3')]},
        c=MultiStep([
            10,
            ['foo', Py('$BATCH + $1')],
            {'name': 'test', 'result': Py('$2[1] + 3')}
        ])
    )
)


def test_task_code_execution():
    assert example_task.execute_adapter(100, None) == {
        'a': 102,
        'b': {'a': 'aa', 'b': ['aa', 103]},
        'c': {'name': 'test', 'result': 113}
    }
