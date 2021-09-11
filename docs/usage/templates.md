# Variables & Templates 

Variables let us define a value outside our DAG. In general it is a good idea not to hard code magic values into our code, and DAGs in Typhoon are no exception. This makes it easier to change or tune the value without requiring us to re-deploy our DAG.

You can set simple Variables or use the same functionality to create Templates. These are useful for:

- variables for logic control 
- boilerplate sql (using jinja or params)
- lists of tables or schemas (e.g. list of tables to run a dag on)

!!! info "Similar to Airflow's variables"
If you're familiar with Airflow you'll notice that it's the same concept as Airflow variables. As much as possible Typhoon tries to depart from tried and tested solutions only when it makes . Variables and connections are something that for the most part just works in Airflow, so we saw no reason to change it adding more cognitive burden on developers.

## Types of variables

With that said it's worth pointing out the differences. In Typhoon variables have types associated with them. If a variable is of type JSON or YAML it will get loaded into a python object. If you need it to be loaded as a string prefer to choose the TEXT type.

  - Options:    ['string', 'jinja', 'number', 'json', 'yaml']

## Quick usage

- To list, load or add  variables in the Typhoon cli use:
    `typhoon variable --help`
- The id you assign or file name used is how you reference it in the DAG
- You can reference your variables with `!Py $VARIABLE.my_variable` or `!Var myvariable` (syntactic sugar)
- As you develop check for issues regularly using `typhoon status`
- List your variables using `typhoon variable ls -l`

## Built-ins

Introducing our built in context variables:

- $BATCH:    This is the data package passed to the next node.
- $BATCH_NUM:    This is the iterator number if you are batching (queries for example).
- $HOOK:    This represents your hook - you can use `!Hook my_conn` as syntax sugar or `!Py $HOOK.myconn`
- $VARIABLE:    This is how you can access saved variables e.g. lists of tables, query templates etc. An example might be `!Py $VARIABLE.mysql_read_3.format(table_name=$BATCH)`
- $DAG_CONTEXT.interval_start & $DAG_CONTEXT.interval_end:     Execution interval for example:
    - $DAG_CONTEXT.interval_start → '2021-05-23 10:00:00'     (inclusive)
    - $DAG_CONTEXT.interval_end → '2021-05-23 11:00:00'     (exclusive)
    
## Examples of setting custom a $VARIABLE

#### Simple Variable

In the typhoon cli use:
` typhoon variable add --var-id weekday_of_archive --var-type string --contents '1'`

You will get a response of `Variable weekday_of_archive added`.

You can reference in a DAG using `!Py $DAG_CONTEXT.interval_start.weekday() == $VARIABLE.weekday_of_archive`.

For example can be used to control if a process runs on a certain weekday with the if component. 

```yaml
name: conditional_process
schedule_interval: rate(10 minutes)

tasks:

  list_tables:
    function: typhoon.flow_control.branch
    args:
      branches: 
        - customers
        - transactions      

  choose_preprocessing:
    input: list_tables
    component: typhoon.if
    args:
      data: !Py $BATCH
      condition: !Py "lambda table: $DAG_CONTEXT.interval_start.weekday() == $VARIABLE.weekday_of_archive"

  archive_day:
    input: choose_preprocessing.then
    function: typhoon.debug.echo
    args:
      data: !MultiStep
        - !Py print('processing archive')
        - !Py $BATCH


  otherday_processing_task:
    input: choose_preprocessing.else
    function: typhoon.debug.echo
    args:
      data: !MultiStep
        - !Py print('processing normal day')
        - !Py $BATCH

```

#### List of tables

In the typhoon cli use the following to add a simple list: 
```bash
typhoon variable add --var-id tables_to_land --var-type yaml  --contents "
- clients
- sales
"
```

!!! info "Adding variables from STDIN"
We can add variables from STDIN by leaving out the --contents argument. This will prompt us to give the value for the variable. We can also pipe the value UNIX style. Eg: cat query_file.sql | typhoon variable add --var-id tutorial_insert_query --var-type jinja or typhoon variable add --var-id tutorial_insert_query2 --var-type string < query_file.sql


#### List of tables from a file

You can also load a yaml file using:  

```bash
typhoon variable load --file "my_table_list.yml"
```

You can reference in a DAG as the name of the file 'my_table_list'. You can then iterate over them like this (using them to load each table for example in future steps):

```yaml
name: variable_example
schedule_interval: rate(10 minutes)

tasks:

  list_tables:
    function: typhoon.flow_control.branch
    args:
      branches: !Py $VARIABLE.my_table_list




  monday_processing_task:
    input: list_tables
    function: typhoon.debug.echo
    args:
      data: !Py $BATCH
```
This will output:

```text
** data = transactions
** data = agents
** data = orders
```

#### SQL Template

We load the following jinja SQL template:
```SQL
SELECT 	* 
FROM {{table_name}}
WHERE 
	start_date >= {{start_date}}
	AND end_date < {{end_date}}
```
Using the following to load, check for issues, and list the variables loaded: 
```
typhoon variable load --file "sql_template_1.sql"`
typhoon status
typhoon variable ls
```

Finally we can add this very easily to the DAG:
```yaml
name: variable_example
schedule_interval: rate(1 hour)

tasks:

  list_tables:
    function: typhoon.flow_control.branch
    args:
      branches: !Py $VARIABLE.my_table_list

  process_sql:
    input: list_tables
    function: typhoon.debug.echo
    args:
      data: !MultiStep
        - !Py $BATCH
        - !Py $DAG_CONTEXT.interval_start
        - !Py $DAG_CONTEXT.interval_end
        - !Py typhoon.templates.render($VARIABLE.sql_template_1, table_name=$1, start_date=$2, end_date=$3)
```

Running this will echo the following for each table in our tables list. This can create powerful automation easily. 
```text
** data = SELECT        *
FROM orders
WHERE
        start_date >= 2021-08-28 16:00:00
        AND end_date < 2021-08-28 17:00:00
;
```

