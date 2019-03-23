from code_execution import run_transformations


def test_run_transformations():
    transformations = [
        "10",
        '2 + $1',
        '$SOURCE * $2',
        "f'{$DAG_CONFIG.ds}: {$3}'"
    ]
    dag_config = {'ds': '2019-03-04'}
    result = run_transformations(source_data=3, dag_config=dag_config, user_transformations=transformations)
    assert result == 15
