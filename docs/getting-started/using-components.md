# Using Components

## What is a component?

[Components][2] are ways of packaging sets of regularly used tasks. This encourages modularity and re-use.

This page focuses on using some packaged example components (rather than [how to construct one][2])

## Example: Archive on Mondays 

Trivial example to choose a different path based on if its a Monday.

`$DAG_CONTEXT.ts` is a datetime object representing the [timestamp][1] of runtime in the context. 

Calling **`component: typhoon.if`** imports the template structure defined in the component YAML. Your constributed components would be referenced `component: components.my_component`.

This `if` component defines two outputs. These are accessed in the line `choose_preprocessing.then` and `choose_preprocessing.else`.

```YAML
name: conditional_process
schedule_interval: rate(10 minutes)

tasks:

  list_files:
    function: typhoon.filesystem.list_directory
    args:
      hook: !Hook ftp
      path: '/'

  choose_preprocessing:
    input: list_files
    component: typhoon.if
    args:
      data: !Py $BATCH
      condition: !Py $DAG_CONTEXT.ts.weekday() == 0

  monday_processing_task:
    input: choose_preprocessing.then
    function: functions.my_process_monday
    args:
      data: !Py $BATCH


  otherday_processing_task:
    input: choose_preprocessing.else
    function: functions.my_process_other_day
    args:
      data: !Py $BATCH
      
  ...
```

## Example: Source DB to Snowflake DWH

This is a non-trivial example of a full DWH load from multipler tables across 3 separate ERP systems.

This calls the entire end to end flow that is packaged in the  **`typhoon.db_to_snowflake`** flow. This component example is fully [idempotent][3], and encapsulating the  of the DHW staging metadata.

```YAML
name: source_to_snowflake
schedule_interval: rate(1 day)

tasks:
  get_sku_erp:
    component: typhoon.db_to_snowflake
    args:
      source_name: sku_component_erp
      source_hook: !Hook ERP
      snowflake_hook: !Hook data_warehouse
      quote_tables: true
      source_tables:
        - sku_list
        - sku_component
        - build_master
        - builds
        - component_extended_desc

  get_transactions:
    component: typhoon.db_to_snowflake
    args:
      source_name: transactions_erp
      source_hook: !Hook trans_ERP
      snowflake_hook: !Hook data_warehouse
      quote_tables: true
      source_tables:
        - transactions
        - client_master
        - shipping
        - supplers
        - deliveries
        - factory_productivity

  get_financials_pnl:
    component: typhoon.db_to_snowflake
    args:
      source_name: finance_erp
      source_hook: !Hook fin_erp
      snowflake_hook: !Hook data_warehouse
      quote_tables: true
      source_tables:
        - payables
        - recievables
        - reconciliations
        - cost_to_serve
  
```

[1]:https://docs.python.org/3/library/datetime.html#datetime.datetime.weekday
[2]:../usage/components.md
[3]:https://medium.com/ssense-tech/lets-get-idempotence-right-59f227178bb8