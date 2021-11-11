# Typhoon Orchestrator Documentation

Welcome to Typhoon!

Typhoon is a Data workflow tool (i.e. ETL / ELT pipelines) for composing **Airflow** DAGs in YAML. 

*Write Airflow DAGS faster*:

`Typhoon YAML DAG --> transpile --> Airflow DAG`

# Help
See the [documentation](https://typhoon-data-org.github.io/typhoon-orchestrator/index.html) or ask in the community [forum](https://typhoon.talkyard.net/latest). 

## Key Principles

- **Elegant** -  YAML; low-code and easy to pick up.
- **Data sharing** -  data flows between tasks making it intuitive and easy to build tasks.
- **Composability** -  Functions combine like Lego. Effortless to extend for more sources and connections.
- **Components** - reduce complex tasks (e.g. CSV→S3→Snowflake) to 1 re-usable task.
- **UI**: Component UI for sharing DAG configuration with your DWH, Analyst or Data Sci. teams.
- **Testable Tasks** - automate DAG task tests.
- **Testable Python** - test functions or full DAGs with PyTest.


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


# Getting started

See [documentation](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html) for detailed guidance. 

## with pip (typhoon standalone)

Install typhoon: 
```bash
pip install typhoon-orchestrator[dev]
```
Optionally, install and activate virtualenv.

Then: 
```bash 
typhoon init hello_world
cd hello_world
typhoon status
```

This will create a directory named hello_world that serves as an example project. As in git, when we cd into the directory it will detect that it's a Typhoon project and consider that directory the base directory for Typhoon (TYPHOON_HOME).

## With Docker and Airflow

To deploy Typhoon with Airflow you need: 

- Docker / Docker Desktop (You must use WSL2 on Windows) 
- Download the [docker-compose.yaml][1]  (or use curl below)
- Create a directory for your TYPHOON_PROJECTS_HOME

The following sets up your project directory and gets the docker-compose.yml:
```bash
TYPHOON_PROJECTS_HOME="/tmp/typhoon_projects" # Or any other path you prefer
mkdir -p $TYPHOON_PROJECTS_HOME/typhoon_airflow_test
cd $TYPHOON_PROJECTS_HOME/typhoon_airflow_test
mkdir src
curl -LfO https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/master/docker-compose-af.yml

docker compose -f docker-compose-af.yml up -d  
docker exec -it typhoon-af bash   # Then you're in the typhoon home.
 
airflow initdb # !! To initiate Airflow DB !!
typhoon status # To see status of dags & connections
typhoon dag build --all # Build the example DAGS
exit # exits docker 
docker restart typhoon-af bash # Wait while docker restarts
```

This runs a container with only 1 service, `typhoon-af`. This has both Airflow and Typhoon installed on it ready to work with.

You should be able to then check `typhoon status` and also the airlfow UI at [http://localhost:8088](http://localhost:8088)

<img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/master/docs/img/airflow_ui_list_after_install.png" width="400">

**Development hints are [in the docs](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html#directories).**



<img src="https://user-images.githubusercontent.com/2353804/112546625-f1cad480-8db9-11eb-8dfb-11e2c8d18a48.jpeg" width="300">

