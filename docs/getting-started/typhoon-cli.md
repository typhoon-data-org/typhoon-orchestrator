# Typhoon cli

## First usage & help
Inspired by other great command line interfaces, it will be instantly familiar to *nix and git users. Intelligent bash/zsh completion.

```bash
typhoon init test_project
typhoon status
typhoon dag ls -l
typhoon dag push test --dag-name example_dag
```

Typing `typhoon` will show the list of options

```bash
  connection  Manage Typhoon connections
  dag         Manage Typhoon DAGs
  extension   Manage Typhoon extensions
  init        Create a new Typhoon project
  metadata    Manage Typhoon metadata
  remote      Manage Typhoon remotes
  status      Information on project status
  variable    Manage Typhoon variables
```

You can use `--help` at each point, for example `typhoon connection --help` will present:

```bash 
Options:
  --help  Show this message and exit.

Commands:
  add         Add connection to the metadata store
  definition  Connection definition in connections.yml
  ls          List connections in the metadata store
  rm          Remove connection from the metadata store
```

## Key cli usage:

- **Starting a new project**:   `typhoon init new_project`   
    - (in your desired directory path)
- **Checking status**:    `typhoon status [ENV]` 
    - This tells you information about the status of your project. Run `typhoon status` and it will find a `typhoon.cfg` file in the current directory. It is assumed that the typhoon project root is the directory that contains the typhoon.cfg.
    - If you want to override that set the environment variable `TYPHOON_HOME` to the full path of the directory.
- **Add a connection**: e.g. `typhoon connection add --conn-id data_lake --conn-env local`
    - Remember to set up your connections in ```connections.yml``` before you add them (data_lake is a default example).
- **Build DAGs**: `typhoon build-dags`. 
    - This will create the folder `out/` in your Typhoon Home directory and also output to Airflow deployment if configured.
- **Run a DAG**: `typhoon run --dag-name hello_world` 
