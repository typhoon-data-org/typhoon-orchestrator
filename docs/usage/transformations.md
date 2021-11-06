# Transformations

Transformations allow you to transform data as it is passed between tasks.

You can do this inline using !Py, in multi-line snippets using !Multistep or encapsulated in a normal python function.  The difference between transformations and functions is that transformations are applied before and can be to a specific argument.

In-line and Multi-line are described in depth in [!Py and !Multistep][1].

Similarly to functions, transformations can be included in the project or referenced from the contributed / built-in repos. 

@ TODO Check this
- `typhoon.transformation_file.transformation_name` for built-in / contrib transformations.
- `transformations.my_transformation_file.transformation_name` for accessing your project transformations. 


### Transformation example

Applying a regex transformation to the data (element 1 which is the path) before its passed to the function. 
```YAML
  write_df_to_sql:
    input: read_csv_to_df
    function: functions.pandas_data_helpers.df_write
    args:
      hook: !Py $HOOK.transaction_db
      df: !Py $BATCH[0]
      table_name: !Py transformations.path_mapping.to_regex_name($BATCH[1])
```

Mixing !Multistep inline transformations and a flatten transformation. The dictionary will be flattened and then passed into csv in two lines before being written by the function. Note you can apply transformations independently to different arguments. In this case data path has a separate in-line transformation using !Py. 

This can make very powerful tasks that are also concise and readable.  

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



[1]: py-multistep.html