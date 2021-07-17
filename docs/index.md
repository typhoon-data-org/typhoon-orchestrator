# Typhoon Orchestrator

Typhoon is a Data Worfklow tool (ETL / ELT pipelines) for composing **Airflow** or AWS Lambda DAGs in YAML. 

**Supercharge your existing Airflow workflow** by using Typhoon to create your DAGs with complete simplicity and re-usability. Deploying to your existing Airflow you can upgrade with zero risk, migrations or loss of existing Airflow functionality.

*If you like Airflow, you will Ariflow + Typhoon more.*

## Key principles
- **Elegant**:  YAML; low-code and easy to pick up.
- **Data sharing** - data flows between tasks making it intuitive and easy to build tasks.
- **Composability** - Functions combine like Lego. Effortless to extend for more sources and connections.
- **Components** - reduce complex tasks (e.g. CSV→S3→Snowflake) to 1 re-usable task.
- **UI**: Component UI for sharing DAG configuration with your DWH, Analyst or Data Sci. teams.
- **Testable Python** - automated PyTest to for more robust flows.
- **Flexible deployment**:
    - deploy to Airflow - large reduction in effort, without breaking existing production.
    - deploy to AWS Lambda - completely serverless

## Example DAG
```yaml
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
Above is an example of two tasks:

1. Extracting the exchange rates from an API call function for a 1-day range
2. Writing CSV to a filesystem. This example actually echos it;  to put it to S3 change the Hook connection name. Within the edge between task 1 and 2 it transforms the data:
    1. It flattens the data 
    2. Then transforms it from a dict to a pipe delimited CSV.
    
## Using with Airflow

Building the above DAG using `typhoon dag build --all` then check the airflow UI for:  
<img src="https://user-images.githubusercontent.com/2353804/112546625-f1cad480-8db9-11eb-8dfb-11e2c8d18a48.jpeg" width="300">



## Component UI

Put in screenshot of UI