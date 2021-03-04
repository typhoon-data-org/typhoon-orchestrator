from pathlib import Path

from typhoon.core.dags import TaskDefinition, Py, MultiStep
from typhoon.core.settings import Settings


def test_task_code_execution():
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
    assert example_task.execute_adapter(100, None, no_custom_transformations=True) == {
        'a': 102,
        'b': {'a': 'aa', 'b': ['aa', 103]},
        'c': {'name': 'test', 'result': 113}
    }


def test_custom_transformation():
    example_task = TaskDefinition(
        function='typhoon.example.f',
        args=dict(
            a=Py('transformations.data.first($BATCH)'),
        )
    )

    import typhoon
    Settings.typhoon_home = Path(typhoon.__file__).parent / 'examples/hello_world'

    assert example_task.execute_adapter(['aa', 'bb', 'cc'], None) == {
        'a': 'aa',
    }
