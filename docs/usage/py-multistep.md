# Inline python 

Typhoon lets you run inline pytnon either in one line `!Py print('Normal python here')` or in multiple lines with `!Multistep`. 

## !Py

This lets you evaluate any python quickly, for example to use pandas or a transformation function.

For example here we are using it to pass the first element of the $BATCH and also to use teh path_mapping regex transformation.   

```YAML
  write_df_to_sql:
    input: read_csv_to_df
    function: functions.pandas_data_helpers.df_write
    args:
      hook: !Hook transaction_db
      df: !Py $BATCH[0]
      table_name: !Py transformations.path_mapping.to_regex_name($BATCH[1])
```

## !Multistep

Sometimes you want to put a few lines in the DAG without making a function for it. You can create powerfull transformations in series in a nice readable way.


In this example we are evaluating each !Py line in the YAML list and referencing the result of the 1st in the second with `$1` (subsequent lines are `$2`, `$3` etc.). 

The result is flattening the response $BATCH data and then outputting as csv to the path that is prefixed with the start timestamp.  

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

```

Another example... 
```YAML
  write_data:
    input: send_data
    function: typhoon.filesystem.write_data
    args:
      hook: !Hook data_lake
      path: !MultiStep
        - !Py $BATCH['filename']
        - !Py $DAG_CONTEXT.interval_end
        - !Py f'/store/{$2}/{$1}'
      data: !Py $BATCH['contents']
      create_intermediate_dirs: True
```

1. `!Py $BATCH['filename']`  will evaluate to "users.txt' (for example). 
2. `!Py $DAG_CONTEXT.interval_end` will evaluate to the timestamp of the DAG run. This is a built in context variable. 
3. `!Py f'/store/{$2}/{$1}'` finally this references the first two lines (1, 2) and uses a normal [Python f-string](https://realpython.com/python-f-strings/) to make the path. Evaluating to '/store/ 2021-03-13T12:00:00/users.txt'