# Hello World Typhoon Project

Serves to show the structure and serve as a template for a Typhoon Project

### Check the status of the project
One of the most important commands is `typhoon status [ENV]` which tells you information about the status of your project. Run `typhoon status` and it will find a `typhoon.cfg` file in the current directory. It is assumed that the typhoon project root is the directory that contains the typhoon.cfg. If you want to override that set the environment variable `TYPHOON_HOME` to the full path of the directory.

### Set the connections
You will notice that typhoon status warns that the connection `data_lake` is not set. Set it as suggested with `typhoon connection add --conn-id singer_cs --conn-env local`.

### Build the project
Run `typhoon build-dags`. This will create the folder `out/` in your Typhoon Home directory.

### Run the Hello World DAG
Do `typhoon run --dag-name example_dag` to run the example DAG.
