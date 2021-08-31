import pytest

from tests.unit.transpiler_tests.transpiler_test_helpers import load_transpiler_test_cases
from typhoon.core.dags import DAGDefinitionV2
from typhoon.core.transpiler.dag_transpiler import DagFile
from typhoon.core.transpiler.task_transpiler import TasksFile


@pytest.mark.parametrize('dag_definition,output', load_transpiler_test_cases('dag_transpiler_test'))
def test_task_transpiler(dag_definition, output):
    dag = DAGDefinitionV2.parse_obj(dag_definition)
    rendered_template = DagFile(dag=dag).render().strip()
    assert rendered_template == output['dag'].strip()

    rendered_template = TasksFile(tasks=dag.tasks).render().strip()
    assert rendered_template == output['tasks'].strip()
