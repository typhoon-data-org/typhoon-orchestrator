import pytest
import yaml
from pydantic import BaseModel, ValidationError
from typhoon.core.dags import add_yaml_constructors, Py, MultiStep

args = """
a: $BATCH + 2
b: !Py $BATCH + 2
c:
    - foo: 5
      bar: 10
    - !Py $BATCH
# d: !Multi
#     - foo: 5
#       bar: !Py 5 + 10
#     - !Py $BATCH + $1['bar']
"""


args = """
a: !Py '"abc"'
b: !Py 54
c: !Hook abc
d: !Var cde
e: !MultiStep
    - foo: 5
      bar: !Py 5 + 10
    - !Py abc
"""


def test_args():
    add_yaml_constructors()
    assert yaml.load(args, yaml.FullLoader) == {
        'a': Py('"abc"'),
        'b': Py('54'),
        'c': Py('$HOOK.abc'),
        'd': Py('$VARIABLE.cde'),
        'e': MultiStep([
            {
                'foo': 5,
                'bar': Py('5 + 10'),
            },
            Py('abc')
        ])
    }


class TestModel(BaseModel):
    x: Py
    y: MultiStep


def test_model_with_py_and_multistep():
    add_yaml_constructors()
    d = yaml.load("""
x: !Py aa
y: !MultiStep
  - a
  - !Py b""", yaml.FullLoader)
    assert TestModel.parse_obj(d) == TestModel(
        x=Py('aa'),
        y=MultiStep(['a', Py('b')])
    )

    with pytest.raises(ValidationError):
        d = yaml.load("""
x: !Py
  - aa
y: !MultiStep
  - a
  - !Py b""", yaml.FullLoader)
        TestModel.parse_obj(d)


def test_transpile_py():
    assert Py('$DAG_CONTEXT.ds + $BATCH + 2').transpile() == 'dag_context.ds + batch + 2'
    x = Py('$BATCH + $2')
    assert x.transpile() == 'batch + $2'
    x.key = 'foo'
    assert x.transpile() == 'batch + foo_2'


def test_transpile_multistep():
    ms = MultiStep(
        value=[
            Py("$BATCH.replace(',', '.')"),
            {'foo': 2, 'bar': Py('$BATCH_NUM')},
            Py('$2[$1]'),
        ],
        key='baz'
    )
    assert ms.transpile() == """\
baz_1 = batch.replace(',', '.')
baz_2 = {'foo': 2, 'bar': batch_num}
baz_3 = baz_2[baz_1]
config['baz'] = baz_3\
"""
