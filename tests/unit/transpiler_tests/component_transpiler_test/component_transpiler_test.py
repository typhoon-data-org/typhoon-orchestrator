import pytest

from tests.unit.transpiler_tests.transpiler_test_helpers import load_transpiler_test_cases
from typhoon.core.components import Component
from typhoon.core.transpiler.component_transpiler import ComponentFile


@pytest.mark.parametrize('component_definition,output', load_transpiler_test_cases('component_transpiler_test'))
def test_task_transpiler(component_definition, output):
    component = Component.parse_obj(component_definition)
    rendered_template = ComponentFile(component=component).render().strip()
    print(rendered_template)
    assert rendered_template == output.strip()
