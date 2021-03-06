<br>
<br>️
<p align="center">
    <img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/f1520188bd639f9a63cf59cdc89d587096d0de4e/docs/img/typhoon_logo_large_tagline_extended.png" >
</p>
<p align="center">
<br>
<br>
 <a target="_blank" href="https://typhoon-data-org.github.io/website/">Website :loudspeaker:</a> |
 <a target="_blank" href="https://discord.gg/XxV5MAF8Xt">Discord :sunglasses:</a> |
 <a target="_blank" href="https://typhoon.talkyard.net/">Forum :wave:</a> |
 <a target="_blank" href="https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation/">Installation :floppy_disk:</a> |
 <a target="_blank" href="https://typhoon-data-org.github.io/typhoon-orchestrator/index.html">Documentation :notebook: </a>

<p align="center">
<img style="margin: 10px" src="https://img.shields.io/github/license/typhoon-data-org/typhoon-orchestrator.svg" alt="Linux" height="20" />
<img style="margin: 10px" src="https://github.com/typhoon-data-org/typhoon-orchestrator/actions/workflows/docker-image.yml/badge.svg" />
</p>



# Why Typhoon?

Our vision is a new generation of cloud native, asynchronous orchestrators that can handle highly dynamic workflows with ease. We crafted Typhoon from the ground up to work towards this vision. It's designed to feel familiar while still making very different design decisions where it matters. 

<p align="center">
<img src="https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/main/docs/img/readme_montage.gif" alt="Typhoon overview montage" >
</p>

## Typhoon + AWS Lambda

A Serverless orchestrator has the potential to be infinitely scalable and extremely cost efficient at the same time. We think AWS Lambda is ideal for this:

- CloudWatch Events can trigger a Lambda on a schedule, so we get scheduling for free! A scheduler is the most complex piece of an orchestrator. We can do away with it completely and still be sure that our DAGs will always run on time.
- Lambda is cheap. You get 1 million invocations for free every month.
- Workflows can be paralellized by running tasks in parallel on different instances of the Lambda. Typhoon DAGs use batching to take full advantage of this.

## Typhoon + Airflow

Airflow is great! 

It's also the industry standard and will be around for a while. However, we think it can be improved, without even migrating your existing production code.  

***Typhoon lets you write Airflow DAGS faster*** :rocket::
  
    **Workflow**: Typhoon YAML DAG --> Typhoon build --> Airflow DAG 

Simplicity and re-usability; a toolkit designed to be loved by Data Engineers :heart:

### Key features

- **Pure python** - Easily extend with pure python. Frameworkless, with no dependencies.
- **Testable Python** - Write tests for your tasks in PyTest. Automate DAG testing. 
- **Composability** - Functions and connections combine like Lego. Very easy to extend.
- **Data sharing** - data flows between tasks making it intuitive to build tasks.
- **Elegant: YAML** - low-code and easy to learn.
- **Code-completion** - Fast to compose. (VS Code recommended).
- **Components** - reduce complex tasks (e.g. CSV → S3 → Snowflake) to 1 re-usable task.
- **Components UI** -  Share your pre-built automation with your team. teams. :raised_hands:
- **Rich Cli & Shell** - Inspired by other great command line interfaces and instantly familiar. Intelligent bash/zsh completion.
- **Flexible deployment** - Deploy to Airflow. Large reduction in effort, without breaking existing production.

# Example YAML DAG
    
```yaml
name: favorite_authors
schedule_interval: rate(1 day)

tasks:
  choose_favorites:
    function: typhoon.flow_control.branch
    args:
      branches:
        - J. K. Rowling
        - George R. R. Martin
        - James Clavell

  get_author:
    input: choose_favorites
    function: functions.open_library_api.get_author
    args:
      author: !Py $BATCH

  write_author_json:
    input: get_author
    function: typhoon.filesystem.write_data    
    args:
      hook: !Hook data_lake
      data:  !MultiStep
        - !Py $BATCH['docs']
        - !Py typhoon.data.json_array_to_json_records($1)
      path: !MultiStep 
        - !Py $BATCH['docs'][0]['key']
        - !Py f'/authors/{$1}.json'
      create_intermediate_dirs: True
```


![Favorite Authors](docs/img/open_library_example_dag.png)
*Getting the works of my favorite authors from Open Library API*


# ⚡ Installation

See [documentation](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html) for more extensive installation instructions and walkthroughs.

## with pip (typhoon standalone)

Install typhoon: 
```bash
pip install typhoon-orchestrator[dev]

# Create a project
typhoon init hello_world

# Try the Cli
cd hello_world
typhoon status

# Add your connection
typhoon connection add --conn-id data_lake --conn-env local
typhoon connection ls -l
```
**Docs**:  [Detailed local installation instructions](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html#directories).  |  [Hello world](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html#directories).


## With Docker and Airflow

To deploy Typhoon with Airflow you need: 

- Docker / Docker Desktop (For now, ***you must use Gitbash on Windows***. Currently, there is an open issue on [WSL2](https://github.com/typhoon-data-org/typhoon-orchestrator/issues/11).) 
- Download the [docker-compose.yaml][1]  (or use curl below)
- Create a directory for your TYPHOON_PROJECTS_HOME

The following sets up your project directory and gets the docker-compose.yml:
```bash
TYPHOON_PROJECTS_HOME="/tmp/typhoon_projects" # Or any other path you prefer
mkdir -p $TYPHOON_PROJECTS_HOME/typhoon_airflow_test
cd $TYPHOON_PROJECTS_HOME/typhoon_airflow_test

# For Windows WSL2 Users - for other env. its optional 
sudo chown -R $USER: $TYPHOON_PROJECTS_HOME/typhoon_airflow_test
mkdir airflow
mkdir data_lake
mkdir src

curl -LfO https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/master/docker-compose-af.yml
```

!!! Important
    On Windows **Gitbash** please run each docker-compose run **one by one**. They are quick.

```bash
docker-compose -f docker-compose-af.yml run --rm typhoon-af airflow initdb
docker-compose -f docker-compose-af.yml run --rm typhoon-af typhoon status
docker-compose -f docker-compose-af.yml run --rm typhoon-af typhoon connection add --conn-id data_lake --conn-env local  # Adding our first connection!
docker-compose -f docker-compose-af.yml run --rm typhoon-af typhoon dag build --all
docker compose -f docker-compose-af.yml up -d
```

This runs a container with only 1 service, `typhoon-af`. This has both Airflow and Typhoon installed on it ready to work with.

You should be able to then check `typhoon status` and also the airlfow UI at [http://localhost:8088](http://localhost:8088)

**Docs**:  [Detailed docker installation instructions](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html#directories).  |  [Development hints](https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/installation.html#directories).

<br>
![Airflow UI](docs/img/airflow_ui_list_after_install.png)
*Typhoon DAGS listed in airflow UI*



![Airflow Favorite Author](docs/img/airflow_favorite_author_basic_dag.PNG)
*Favorite Authors DAG - as displayed in airflow UI*

