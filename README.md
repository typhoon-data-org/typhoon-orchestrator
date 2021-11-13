
<p align="center">
<br>
<a href="http://discord.skerritt.blog">Forum :wave:</a> |
 <a href="https://github.com/RustScan/RustScan/wiki/Installation-Guide">Installation :floppy_disk:</a> |
 <a href="https://github.com/RustScan/RustScan#-usage">Documentation :notebook: </a>
<br>️
<br>
<br>️
<img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/feature/docs_gitpages/docs/img/typhoon_logo_large_tagline.png" >
</p>
<br>
<p align="center"><b>Elegant YAML DAGS for Data Pipelines</br>Deploy to your existing Airflow.</b></p>
<br>️
<p align="center">
<img alt="AUR version" src="https://img.shields.io/aur/version/rustscan">
<img src="https://img.shields.io/badge/Built%20with-Rust-Purple">
<img alt="GitHub All Releases" src="https://img.shields.io/github/downloads/rustscan/rustscan/total?label=GitHub%20Downloads">
<img alt="Crates.io" src="https://img.shields.io/crates/d/rustscan?label=Cargo%20Downloads">
<img alt="Discord" src="https://img.shields.io/discord/754001738184392704">
<img alt="Actions" src="https://github.com/RustScan/RustScan/workflows/Continuous%20integration/badge.svg?branch=master">
</p>

<p align="center">
<br>
<a href="http://discord.skerritt.blog">Why Typhoon</a> |
 <a href="https://github.com/RustScan/RustScan/wiki/Installation-Guide">Key Features</a> |
 <a href="https://github.com/RustScan/RustScan#-usage">Example YAML</a> |
 <a href="https://github.com/RustScan/RustScan#-usage">Installation</a>
<br>️


<hr>

# Why Typhoon (+ Airflow)?

Airflow is great!

***Typhoon lets you write Airflow DAGS faster*** :rocket::
  
    **Workflow**: Typhoon YAML DAG --> Typhoon build --> Airflow DAG 
  
# Key features
<table style="border: none" cellspacing="0" cellpadding="0">
<tr>
<td width="50%">

**Elegant** -  YAML; low-code and easy to learn.

**Code-completion** - Fast to compose.

**Data sharing** -  data flows between tasks making it super intuitive.

**Composability** -  Functions and connections combine like Lego.


</td>
<td><img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/feature/docs_gitpages/docs/img/component_ui.gif" alt="UI Component" width="400px" align="right" style="max-width: 100%;">
</td>
</tr>
<tr>
<td width="50%">

**Components** - reduce complex tasks to 1 re-usable tasks

(e.g. CSV → S3 → Snowflake).

**UI**: share your pre-built components (data pipelines) for your team to use.

</td>
<td><img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/feature/docs_gitpages/docs/img/component_ui.gif" alt="UI Component" width="400px" align="right" style="max-width: 100%;">
</td>
</tr>
<tr>
<td width="50%">

- **Rich CLI & Shell**: Inspired by others; instantly familiar.
- **Testable Tasks** - automate DAG task tests.
- **Testable Python** - test functions or full DAGs with PyTest.

</td>
<td><img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/feature/docs_gitpages/docs/img/component_ui.gif" alt="UI Component" width="400px" align="right" style="max-width: 100%;">
</td>
</tr>

</table>

See the [documentation](https://typhoon-data-org.github.io/typhoon-orchestrator/index.html) or ask in the community [forum](https://typhoon.talkyard.net/latest). 

# Example YAML DAG
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
      hook: !Hook xr_s3_bucket
      data: !MultiStep
        - !Py transformations.xr.flatten_response($BATCH)
        - !Py typhoon.data.dicts_to_csv($1, delimiter='|')
      path: xr_data.csv
```

Above is an example of two tasks:

1. Extracting the exchange rates from an API call function for a 1-day range
2. Writing CSV to a filesystem; S3 in this case. Within the edge between task 1 and 2 it transforms the data:
    1. It flattens the data 
    2. Then transforms it from a dict to a pipe delimited CSV.


# Getting started

See [documentation](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html) for detailed guidance on installation and walkthroughs. 

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

