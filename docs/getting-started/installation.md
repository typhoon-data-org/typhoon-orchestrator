# Installation
Typhoon can be installed locally with pip or using docker. To test airflow (especially on Windows) we recommend using the docker version. 

!!! tip "Use the DEV version when installing it locally."
    The [dev] version comes with all the libraries and tools that make development easier.

        `pip install typhoon-orchestrator[dev]`

    The production version is lightweight for use with Lambda.


## Installation

### with pip 

```bash
pip install typhoon-orchestrator[dev]
```

#### Creating your new project

Inside your terminal navigate to where you want to create your new project directory. Then run:

```bash 
typhoon init hello_world
cd hello_world
```

This will create a directory named hello_world that serves as an example project. As in git, when we cd into the directory it will detect that it's a Typhoon project and consider that directory the base directory for Typhoon (TYPHOON_HOME).

#### Checking 'typhoon status'


```bash
typhoon status
```

Result:

<img src="../img/Screenshot%202021-07-17%20192117.png">

We can see that it's detecting the project home as well as a SQLite metadata database that just got created. It's also warning us that our DAG uses a connection that is not defined in the metadata database and suggesting us a fix.

We will see in the next section 'Connections' how to add these. 

!!! tip "Bash/ZSH/Fish auto-complete" 
    - bash eval "$(_TYPHOON_COMPLETE=source_bash typhoon)"
    - zsh eval "$(_TYPHOON_COMPLETE=source_zsh typhoon)"
    - fish eval "$(_TYPHOON_COMPLETE=source_fish typhoon)"

## with docker

@TODO DOCKER update - also in Readme.


## with docker and Airflow

@TODO DOCKER update - also in Readme.