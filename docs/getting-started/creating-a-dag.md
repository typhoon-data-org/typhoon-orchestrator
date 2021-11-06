# Creating a DAG

Simply, the DAG is the workflow task you are composing. This is a DAG structure similar to many other workflow tools (e.g. Task A → Task B → Task C). 

However, the writing of a DAG in Typhoon can be much quicker and more intuitive due to the simpler YAML syntax and that it shares data. 

Concepts: 

- tasks: this defines each node (or step) in the DAG workflow (i.e. do something).
- [functions][4]: this is the operation to carry out (e.g. read data, write data, branches, if, etc.).  
- [transformations][5]: stored functions that are re-usable and testable (e.g. gzip, dicts_to_csv). 
- [components][6]: re-usable sets of multi-task DAGS (e.g. glob_compress_and_copy, if, db_to_swowflake). Can be used as standalone or in a task.


## DAG code
We can check an example DAG in `dags/hello_world.yml`. You can also check it with the CLI by running `typhoon dag definition --dag-name hello_world`.

!!! tip
    To get some editor completion and hints you can configure the JSON schema in `dag_schema.json` as the schema for the DAG YAML file. This can be done in VS Code with the [Red Hat YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) or in [PyCharm natively](https://www.jetbrains.com/help/pycharm/json.html).

```yaml
name: hello_world
schedule_interval: rate(1 hours)  # Can also be a Cron expression
granularity: hour

tasks:
  send_data:
    function: typhoon.flow_control.branch
    args:
      branches:
        - filename: users.txt
          contents: John, Amy, Adam, Jane
        - filename: animals.txt
          contents: dog, cat, mouse, elephant, giraffe
        - filename: fruits.csv
          contents: apple,pear,apricot

  write_data:
    input: send_data
    function: typhoon.filesystem.write_data
    args:
      hook: !Hook data_lake
      path: !MultiStep
        - !Py $BATCH['filename']
        - !Py $DAG_CONTEXT.interval_end
        - !Py f'/store/{$2}/{$1}'
      data: !Py $BATCH['contents']
      create_intermediate_dirs: True
```

This DAG has three branches, each with a file name and list (YAML list of dictionaries). This will write 3 files; users.txt, animals.txt and fruits.csv.  

Typically, this first step might be a database, set of CSVs or an API. This is just a trivial example including data.

!!! info "Skim the code"
    For a step-by-step  on this example - please start on our example [Hello World walkthrough][1].

## DAG basic concepts
The DAGs are composed in [YAML][2]. They are compiled to python. This means that once you have written your YAML and you build it the output is a normal, testable python file. It transpiles to Airflow compatible DAGS also. 

Basic components:

- name: ...
- schedule_interval: can be a [Rate or a Cron expression][3] to express how often it should run.   
- granularity: minutes, hour, hours, day, days ... or a Cron expression.
- tasks: this defines each node (or step) in the DAG workflow (i.e. do something) 
    - input: here we are connecting the input from the previous task (A -> B; B.input = A).
    - function: this is the operation to carry out (e.g. read data, write data, branches, if, etc.). Full [function list][4] for reference. 
    - args: these are the specific arguments for the function (so they will differ). Common ones are below:
        - hook: this is the connection to use that you have added from your `connectioons.yml` 
        - path: this is the path, typical in filesystem hooks to read / write files
        - data: this is the batch of data we are passing between tasks, referenced by **`$BATCH`**
    
Syntax sugars:
- !Hook: e.g. `!Hook data_lake` is equivilent of `$HOOK.data_lake`. I.e. to get the connection. 
- !MultiStep: allows you to make multi-line scripts easily (see example). Useful for chaining together a few Pandas transformations for example. 
- !Py: this evaluates the line as normal python (allowing total flexibility therefore to apply your own transformations).



## Running the DAG

We can run this with:

```bash
typhoon dag run --dag-name hello_world
```

We can check the files that have been written to `/tmp/data_lake/store/` where `/tmp/data_lake/` was the base directory defined for our connection (go back to `connections.yml` to check) and the DAG wrote to `/store/[FILE_NAME]`.


[1]:/hello.md
[2]:https://docs.ansible.com/ansible/latest/reference_appendices/YAMLSyntax.html
[3]:https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-expressions.html
[4]:/hello.md
[5]:/hello.md
[6]:/hello.md