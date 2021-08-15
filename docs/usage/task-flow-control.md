# Task flow control

Basic usage of making of a trivial DAG of `read_data -> write_data` is expressed by naming the tasks and linking them with the `input` argument. 

```YAML
name: write_to_csv
schedule_interval: rate(1 hours)
granularity: hour

tasks:
  read_data:
    function: functions.filesystem.read_data
    args:
        hook: !Hook files
        path: /iris.data

  write_data:
    input: read_data
    function: typhoon.filesystem.write_data
    args:
      hook: !Hook files
      data: !Py $BATCH
      path: !Py f"/out.csv"
```

However, we may also have more complex requirements of the logic flow.

In this case the `typhoon.flow_control` functions are useful. 
- `branch`  - to yield over each item in a sequence.
- `filter`  - to return only items that are True, skipping the rest

## Branching

In some cases you need to make more advanced logic that iterates over many items (e.g. multiple tables in a DB to land in a DWH).

Using the function `typhoon.flow_control.branch` we can yield each table from VARIABLES and run `write_date` (and all tasks downstream). 

```YAML
name: read_many_tables
schedule_interval: rate(1 hours)
granularity: hour
  
  list_tables:
    function: typhoon.flow_control.branch
    args:
      branches: !Var sql_tables_list_1
  
  write_data:
    input: list_tables
    function: typhoon.relational.execute_query
    args:
      hook: !Hook transaction_db
      batch_size: 10
      metadata:
        table_name: !Py $BATCH
      query: !Py $VARIABLE.mysql_read_3.format(table_name=$BATCH)
```

## Filter

We can easily apply a simple condition using `typhoon.flow_control.filter`. Items matching the condition will be passed to the next task while the rest skipped. 

This example shows how it can be used to filter those ending with `.csv` which are then passed to read them using Pandas and finally written to sql. You could easily make this into a Component which can use the UI. 

```YAML
name: pickup_data_to_sql
schedule_interval: rate(10 minutes)

tasks:

  list_files:
    function: typhoon.filesystem.list_directory
    args:
      hook: !Hook ftp
      path: '/'

  filter_csv_files:
    input: list_files
    function: typhoon.flow_control.filter
    args:
      filter_func: !Py "lambda x: x.endswith('.csv')"
      data: !Py $BATCH

  read_csv_to_df:
    input: filter_csv_files
    function: functions.pandas_data_helpers.csv_to_df
    args:
      hook: !Py $HOOK.ftp
      path: !Py $BATCH

  write_df_to_sql:
    input: read_csv_to_df
    function: functions.pandas_data_helpers.df_write
    args:
      hook: !Py $HOOK.transaction_db
      df: !Py $BATCH[0]
      table_name: !Py transformations.path_mapping.to_regex_name($BATCH[1])


tests:
  read_csv_to_df:
    batch: '/file.csv'
    expected:
      path: '/file.csv'

  write_df_to_sql:
    batch:
      - !Py "pd.DataFrame({'a': [1,2,3,4], 'b': [6,7,8,9]})"
      - "data_clients_2021-02-18T03_00_00.csv"
    expected:
      df: !Py "pd.DataFrame({'a': [1,2,3,4], 'b': [6,7,8,9]})"
      table_name: "clients"
```