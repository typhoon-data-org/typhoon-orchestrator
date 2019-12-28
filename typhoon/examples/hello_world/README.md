# Hello World Typhoon Project

Serves to show the structure and serve as a template for a Typhoon Project

### Check the status of the project
One of the most important commands is `typhoon status ENV` which tells you information about the status of your project. Run `typhoon status dev` and it will tell you that you should set up TYPHOON_HOME variable. Set it to the full path of the hello_world/ directory.

### Build the project
Run `typhoon build-dags dev --debug`. This will create the folder out/ in your Typhoon Home directory.

### Run the Hello World DAG
Do `typhoon run example_dag dev` to run the example DAG.
