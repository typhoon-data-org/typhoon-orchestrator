# Typhoon Orchestrator

Typhoon is a Data Worfklow tool (ETL / ELT pipelines) for composing **Airflow** or AWS Lambda DAGs in YAML. 

**Supercharge your existing Airflow workflow** by using Typhoon to create your DAGs with complete simplicity and re-usability. Deploying to your existing Airflow you can upgrade with zero risk, migrations or loss of existing Airflow functionality.

*If you like Airflow, you will Ariflow + Typhoon more.*

**Workflow**: Typhoon YAML DAG --> transpile --> Airflow deployment 

??? info "Key Principles"     
    - **Elegant**:  YAML; low-code and easy to pick up.
    - **Data sharing** - data flows between tasks making it intuitive and easy to build tasks.
    - **Composability** - Functions combine like Lego. Effortless to extend for more sources and connections.
    - **Components** - reduce complex tasks (e.g. CSV→S3→Snowflake) to 1 re-usable task.
    - **UI**: Component UI for sharing DAG configuration with your DWH, Analyst or Data Sci. teams.
    - **Rich Cli**: Inspired by other great command line interfaces and instantly familiar. Intelligent bash/zsh completion.
    - **Testable Python** - automated PyTest to for more robust flows.
    - **Flexible deployment**:
        - deploy to Airflow - large reduction in effort, without breaking existing production.
        - deploy to AWS Lambda - completely serverless

### Layers 

- **Pristine**: Pre-built (OSS) components and UI that can be shared to your team    
- **Core**: Python (OSS) core 
    - Extensible and hackable.
    - Components allow you to share extensions widely in the Pristine layer.    


## Example DAG

=== "Typhoon DAG (YAML)"

    ```yaml linenums="1"
    name: exchange_rates
    schedule_interval: rate(1 day)
    
    tasks:
      exchange_rate:
        function: functions.exchange_rates_api.get_history
        args:
          start_at: !Py $DAG_CONTEXT.interval_start
          end_at: !Py $DAG_CONTEXT.interval_end
    
      write_csv:
        input: exchange_rate
        function: typhoon.filesystem.write_data
        args:
          hook: !Hook echo
          data: !MultiStep
            - !Py transformations.xr.flatten_response($BATCH)
            - !Py typhoon.data.dicts_to_csv($1, delimiter='|')
          path: xr_data.csv
    ```

=== "Airflow DAG (python)"

    ```python linenums="1"
    import datetime
    
    import typhoon.contrib.functions as typhoon_functions   # for a fair comparison
    import typhoon.contrib.transformations as typhoon_transformations
    from airflow import DAG
    from airflow.hooks.base_hook import BaseHook
    from airflow.operators.python_operator import PythonOperator
    
    import transformations.xr
    from functions import exchange_rates_api
    from out.new_people.typhoon.contrib.hooks.filesystem_hooks import LocalStorageHook
    
    def exchange_rate(base: str, **context):
        result = exchange_rates_api.get_history(
            start_at=context['execution_date'],
            end_at=context['next_execution_date'],
            base=base,
        )
        context['ti'].xcom_push('result', list(result))
    
    def write_csv(source_task_id, **context):
        conn_params = BaseHook.get_connection('data_lake')
        hook = LocalStorageHook(conn_params)       # Note how we're hardcoding the class
        
        batches = context['ti'].xcom_pull(task_ids=source_task_id, key='result')
        for batch in batches:
            flattened = transformations.xr.flatten_response(response)
            data = typhoon_transformations.data.dicts_to_csv(flattened, delimiter='|')
            path = context['ds'] + '_xr_data.csv'
            typhoon_functions.filesystem.write_data(
                hook=hook,
                data=data,
                path=path
            )
    
    with DAG(
        dag_id='exchange_rates',
        default_args={'owner': 'typhoon'},
        schedule_interval='*/1 * * * *',
        start_date=datetime.datetime(2021, 3, 25, 21, 10)
    ) as dag:
        for base in ['EUR', 'USD', 'AUD']:
            exchange_rate_task_id = f'exchange_rate_{base}'
            exchange_rate_task = PythonOperator(
                task_id=exchange_rate_task_id,
                python_callable=exchange_rate,
                op_kwargs={
                    'base': base
                },
                provide_context=True
            )
            dag >> exchange_rate_task
    
            write_csv_task = PythonOperator(
                task_id=f'write_csv_{base}',
                python_callable=write_csv,
                op_kwargs={
                    'source_task_id': exchange_rate_task_id
                },
                provide_context=True
            )
            exchange_rate_task >> write_csv_task
    ```

Above is an example of two tasks:

1. Extracting the exchange rates from an API call function for a 1-day range
2. Writing CSV to a filesystem. This example actually echos it;  to put it to S3 change the Hook connection name. Within the edge between task 1 and 2 it transforms the data:
    1. It flattens the data 
    2. Then transforms it from a dict to a pipe delimited CSV.
    
## Using with Airflow

Building the above DAG using `typhoon dag build --all`. 

Airflow UI will then show:  
<img src="https://user-images.githubusercontent.com/2353804/112546625-f1cad480-8db9-11eb-8dfb-11e2c8d18a48.jpeg" width="300">

## Component UI

Put in screenshot of UI


