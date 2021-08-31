import pytest

from tests.unit.transpiler_tests.transpiler_test_helpers import load_transpiler_test_cases
from typhoon.core.dags import TaskDefinition
from typhoon.core.transpiler.task_transpiler import TaskTemplate


@pytest.mark.parametrize('task,output', load_transpiler_test_cases('task_transpiler_test'))
def test_task_transpiler(task, output):
    task_id = list(task.keys())[0]
    task = TaskDefinition.parse_obj(task[task_id])
    rendered_template = TaskTemplate(task_id=task_id, task=task).render().strip()
    print(rendered_template)
    assert rendered_template == output.strip()
