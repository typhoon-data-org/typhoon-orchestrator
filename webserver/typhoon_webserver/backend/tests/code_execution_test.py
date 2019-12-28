from code_execution import run_transformations


def test_run_transformations():
    transformations = [
        "10",
        '2 + $1',
        '$BATCH * $2',
        "f'{$DAG_CONTEXT.ds}: {$3}'"
    ]
    dag_context = {'ds': '2019-03-04'}
    result = run_transformations(source_data=3, dag_context=dag_context, user_transformations=transformations)
    assert result == 15
