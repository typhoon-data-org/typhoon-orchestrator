# Typhoon Orchestrator

![Alt text](docs/img/typhoon_temp_logo.png "Typhoon Orchestrator")

**Typhoon** is a task orchestrator (like Apache Airflow). 

Use simple YAML code to easily build **asynchronous** data pipelines for your ELT workflows. 

Save huge amounts of effort by building your data pipelines in Typhoon and then **deploy them to run on Airflow;** this means you have a risk free, no-lock-in experience. 

This project is in **pre-alpha** and subject to fast changes

# Why Typhoon for data flows?

- Elegant YAML and Components - 'low-code', easy to pick up.
- Compile to testable Python - robust flows.
- Functions combine like Lego - effortless to extend for more sources and connections.
- Compose *Components* - reduce repetitive complex tasks (e.g. CSV→S3→Snowflake) to one-line of code.
- Data flows between tasks - intuitive and easy to build tasks.
- Flexible deployment options:
    - **deploy to Airflow** - massively reducing effort, without breaking existing production.
    - deploy to AWS Lambda - **completely serverless**

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

1. Extracting the exchange rates from an API call function for a 1 day range
2. Writing CSV to a filesystem. This example actually echos it - but its just changing the Hook connection name to put it to S3. Within the edge between task 1 and 2 it transforms the data:
    1. It flattens the data 
    2. Then transforms it from a dict to a pipe delimited CSV.

The syntax and functionality is covered fully in the tutorials 1 & 2 below.

# Tutorials & Examples included

- [hello_world.yml  -  list of lists → to CSV](https://www.notion.so/Part-1-Typhoon-HelloWorld-6d4b6a6e778e4906ac7e502dce69fd13)     [**(Tutorial Part 1)**](https://www.notion.so/Part-1-Typhoon-HelloWorld-6d4b6a6e778e4906ac7e502dce69fd13)
- [dwh_flow  -  Production ready MySQL  tables → Snowflake DWH pattern](https://www.notion.so/Part-2-MySQL-to-Snowflake-8cfaeac1f5334138b93a981d82c9532f)    [(Tutorial Part 2)](https://www.notion.so/Part-2-MySQL-to-Snowflake-8cfaeac1f5334138b93a981d82c9532f)
- exchange_rates.yml  -   (above) API → CSV on S3
- telegram_scraper.yml  -  telegram group → CSV

# Getting started

## Docker

```bash
docker run -it biellls/typhoon bash

# Running isolated commands
docker run --rm biellls/typhoon typhoon status
```

## Local installation

```bash
# Install and activate virtualenv
python3 -m venv typhoon_venv
./typhoon_venv/bin/activate

# Clone and install from source
git clone https://github.com/biellls/typhoon-orchestrator.git
cd typhoon-orchestrator
python -m pip install ./typhoon-orchestrator[dev]
# Activate bash complete
eval "$(_TYPHOON_COMPLETE=source typhoon)"

# Create a typhoon project
typhoon init typhoon_project
cd typhoon_project
```

### Testing with airflow

```bash
git clone https://github.com/biellls/typhoon-orchestrator.git
cd typhoon-orchestrator
docker build -f Dockerfile.af -t typhoon-af .

TYPHOON_PROJECTS_HOME = "~/typhoon_projects" # Or any other path you prefer
mkdir -p $TYPHOON_PROJECTS_HOME/typhoon_airflow_test
cd $TYPHOON_PROJECTS_HOME/typhoon_airflow_test

typhoon init my_typhoon_project --deploy-target airflow --template airflow_docker
cd my_typhoon_project
docker compose up -d
docker exec -it typhoon-af bash   # Then you're in the typhoon home. Try typhoon status

```
