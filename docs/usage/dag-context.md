# DAG Context, $Batch etc. 

Typhoon has some built in context variables: 

- $BATCH:    This is the data package passed to the next node.
- $BATCH_NUM:    This is the iterator number if you are batching (queries for example).
- $HOOK:    This represents your hook - you can use `!Hook my_conn` as syntax sugar or `!Py $HOOK.myconn`
- $VARIABLE:    This is how you can access saved variables e.g. lists of tables, query templates etc. An example might be `!Py $VARIABLE.mysql_read_3.format(table_name=$BATCH)`
- $DAG_CONTEXT.interval_start & $DAG_CONTEXT.interval_end:     Execution interval for example:
    - $DAG_CONTEXT.interval_start → '2021-05-23 10:00:00'     (inclusive)
    - $DAG_CONTEXT.interval_end → '2021-05-23 11:00:00'     (exclusive)
    

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


## $BATCH

This is the data package passed to the next node and can access the various features of the payload. 

For example if the data being passed is a JSON array:

```json
{
  "filename": "my_file.csv",
  "contents": "a, b, c, d ...etc"
}
``` 
You can access these in the following way. In this example we are using the previous step's data for the path and the data step separately.  

```YAML
  write_data:
    input: send_data
    function: typhoon.filesystem.write_data
    args:
      hook: !Hook data_lake
      path: !MultiStep
        - !Py $BATCH['filename'] 
        - !Py f'/store/{$1}'
      data: !Py $BATCH['contents']
      create_intermediate_dirs: True
```

## $BATCH_NUM

This is the iterator number if you are batching (queries for example).

```YAML
  load:
    input: extract
    function: typhoon.filesystem.write_data
    args:
        hook: !Hook data_lake
        data: !Py typhoon.transformations.write_csv($BATCH.data)
        path: !Py f'{$BATCH.table_name}/part{$BATCH_NUM}.csv'
```

## $HOOK

$HOOK represents your hook - you can use `!Hook my_conn` as syntax sugar or `!Py $HOOK.myconn`

## $VARIABLE

This is how you can access saved variables e.g. lists of tables, query templates etc. 

To add a variable use the typhoon cli with `typhoon variable add`. Full list of options is available with `typhoon variable --help`.

To call the variable, you can use `!Var myvariable` syntactic sugar or `$VARIABLE.myvariable` for example this DAG has 2 usages:
- `!Var sql_tables_list_1["my_db_tables"]`    (getting the list of tables)
- `!Py $VARIABLE.mysql_read_3.format(table_name=$BATCH)`    (using a sql string .format() template)

```YAML
name: dwh_flow
schedule_interval: rate(1 hours)

tasks:

  list_tables:
    function: typhoon.flow_control.branch
    args:
      branches: !Var sql_tables_list_1["my_db_tables"]

  extract_tables:
    input: list_tables
    function: typhoon.relational.execute_query
    args:
      hook: !Py $HOOK.transaction_db
      #hook: !Py $HOOK.echo
      batch_size: 10
      query_params:
        table_name: "tablen"
      metadata:
        table_name: !Py $BATCH
      query: !Py $VARIABLE.mysql_read_3.format(table_name=$BATCH)

  debug_1:
    input: extract_tables
    function: functions.debug.my_echo
    args:
      x: !Py $BATCH
```

## $DAG_CONTEXT

- `interval_start`  - execution interval  (e.g.'2021-05-23 10:00:00'  inclusive)
- `interval_end` - execution interval (e.g.'2021-05-23 10:00:00'  exclusive)
- ...

