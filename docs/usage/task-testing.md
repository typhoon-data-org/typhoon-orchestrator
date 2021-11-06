# Testing

With Typhoon you can test your DAG tasks by including tests into the YAML. This is very helpful for development. It allows you to see how the task might react to different data structures. 

You can also write [PyTest][1] tests for you functions, transformations, and hooks in the normal way that you would for any other code. This is allows you to write robust flows. These are included in the `tests` folder, either as a unit or integration test.   

You can also debug and test the DAG compiled python code in the normal way. 

## DAG Task Test

You can add a set of tests (as many as you like) in the DAG at the bottom using the key `tests` and then the name of the task you are testing.

You need to provide the input data into `batch` and the `interval_start` and `interval_end`. The test then runs using this input (i.e. not running data from preceding tasks) and compares the output to the `expected` key. In `expected` you need to assign the keys that the function outputs. 


# @TODO BELOW - Fix after refactor and get a better examples



```YAML
  write_csv:
    input: exchange_rate
    function: typhoon.filesystem.write_data
    args:
      hook: !Hook echo
      data: !MultiStep
        - !Py transformations.xr.flatten_response($BATCH)
        - !Py typhoon.data.dicts_to_csv($1, delimiter='|')
      path: !Py f'{$DAG_CONTEXT.interval_start.isoformat()}_xr_data.csv'


tests:
  write_csv:
    batch:
      a: "a_pay"
      b: "b_pay"
    interval_start: 2021-02-03T01:01:01
    interval_end: 2021-02-03T02:01:01
    expected:
      data: '[{"cola": 1, "colb": 2, "colc": 3}, {"cola": 2, "colb": 4, "colc": 5}]'
      path: 'data_abc_batch_num_2_2021-02-03T01_01_01.json'

```


[1]: https://docs.pytest.org