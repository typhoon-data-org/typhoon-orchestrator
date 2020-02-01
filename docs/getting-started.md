# Getting started

To install typhoon for development do

```bash
pip install typhoon-orchestrator[dev]
```

You always want the DEV version when installing it locally since it comes will all the libraries and tools that make development easier.

!!! error "Don't install the basic version"
    The basic version that you would get with `pip install typhoon-orchestrator` is the lightweight version that runs inside lambda in production because it is much more light-weight.

## New project

Inside your terminal run:

```bash
typhoon init hello_world
cd hello_world
```

This will create a directory named hello_world that serves as an example project. As in git, when we cd into the directory it will detect that it's a Typhoon project and consider that directory the base directory for Typhoon (TYPHOON_HOME).

## Checking status

As in git, one of the most important commands is status. It gives us information about the state of the project as well as tips on how to fix problems or warnings.

```bash
typhoon status
```

Result:
```text
 _________  __  __   ______   ___   ___   ______   ______   ___   __
/________/\/_/\/_/\ /_____/\ /__/\ /__/\ /_____/\ /_____/\ /__/\ /__/\
\__.::.__\/\ \ \ \ \\:::_ \ \\::\ \\  \ \\:::_ \ \\:::_ \ \\::\_\\  \ \
   \::\ \   \:\_\ \ \\:(_) \ \\::\/_\ .\ \\:\ \ \ \\:\ \ \ \\:. `-\  \ \
    \::\ \   \::::_\/ \: ___\/ \:: ___::\ \\:\ \ \ \\:\ \ \ \\:. _    \ \
     \::\ \    \::\ \  \ \ \    \: \ \\::\ \\:\_\ \ \\:\_\ \ \\. \`-\  \ \
      \__\/     \__\/   \_\/     \__\/ \::\/ \_____\/ \_____\/ \__\/ \__\/

• Typhoon home defined as /Users/biellls/Desktop/typhoon/hello_world
• Metadata database found in sqlite:/Users/biellls/Desktop/typhoon/hello_world/hello_world.db
• Found connections in YAML that are not defined in the metadata database
   - Connection data_lake is not set. Try typhoon connection add --conn-id data_lake --conn-env CONN_ENV
• Found connections in DAGs that are not defined in the metadata database
   - Connection data_lake is not set. Try typhoon connection add --conn-id data_lake --conn-env CONN_ENV
• All variables in the DAGs are defined in the database
• DAGs up to date
```

We can see that it's detecting the project home as well as a SQLite metadata database that just got created. It's also warning us that our DAG uses a connection that is not defined in the metadata database and suggesting us a fix.

!!! tip "Bash/ZSH complete"
    To get bash complete for the project run `eval "$(_TYPHOON_COMPLETE=source typhoon)"` and add that into `~/.bashrc`. For zsh run `eval "$(_TYPHOON_COMPLETE=source_zsh typhoon)"` and add that into `~/.zshrc`

## Connections
### Connections YAML

During development we may want to use different connections to test our DAGs. For example we may start development writing to a local file and after that is working correctly switch the connection to S3 to finish testing. We may also want to keep the production connection details somewhere handy.

For this specific purpose there is a file called `connections.yml` in our project where we will define all our connection details. Furthermore, for one connection name we can define different connection environments, each with their own connection. For example in this project we will write data using a connection called `data_lake` which has two connection environments, `local` which writes to a local file and `test` which writes to S3. It is possible to use them interchangeably since they both implement the same interface: `FileSystemHook`.


**connections.yml:**
```yaml
data_lake:
  test:
    conn_type: s3
    extra:
      bucket: my-typhoon-test-bucket

  local:
    conn_type: local_storage
    extra:
      base_path: /tmp/data_lake/
```

!!! note "connections.yml is not versioned"
    The `connections.yml` file may contain passwords so it should never be versioned. That is why it's included in the `.gitignore` file for projects generated with the CLI.

### Adding the connection

Following the advice we got from the `typhoon status command` we will now add the connection to the metadata database. We will add the local data_lake connection with the command:

```bash
typhoon connection add --conn-id data_lake --conn-env local
# Check that it was added
typhoon connection ls -l
```

If we run the status command again we will see that everything is ok in our project now:

```bash
typhoon status
```

```text
 _________  __  __   ______   ___   ___   ______   ______   ___   __
/________/\/_/\/_/\ /_____/\ /__/\ /__/\ /_____/\ /_____/\ /__/\ /__/\
\__.::.__\/\ \ \ \ \\:::_ \ \\::\ \\  \ \\:::_ \ \\:::_ \ \\::\_\\  \ \
   \::\ \   \:\_\ \ \\:(_) \ \\::\/_\ .\ \\:\ \ \ \\:\ \ \ \\:. `-\  \ \
    \::\ \   \::::_\/ \: ___\/ \:: ___::\ \\:\ \ \ \\:\ \ \ \\:. _    \ \
     \::\ \    \::\ \  \ \ \    \: \ \\::\ \\:\_\ \ \\:\_\ \ \\. \`-\  \ \
      \__\/     \__\/   \_\/     \__\/ \::\/ \_____\/ \_____\/ \__\/ \__\/

• Typhoon home defined as /Users/biellls/Desktop/typhoon/hello_world
• Metadata database found in sqlite:/Users/biellls/Desktop/typhoon/hello_world/hello_world.db
• All connections in YAML are defined in the database
• All connections in the DAGs are defined in the database
• All variables in the DAGs are defined in the database
• DAGs up to date
```

!!! tip "Use bash complete"
    Typhoon CLI has intelligent completion for a lot of commands and arguments so make sure to hit TAB whenever you want some help. It can suggest connection IDs and connection environments from the `connections.yml` file.

## DAG

### DAG code
We can check an example DAG in `dags/hello_world.yml`. You can also check it with the CLI by running `typhoon dag definition --dag-name hello_world`.

!!! tip
    To get some editor completion and hints you can configure the JSON schema in `dag_schema.json` as the schema for the DAG YAML file. This can be done in VS Code with the [Red Hat YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) or in [PyCharm natively](https://www.jetbrains.com/help/pycharm/json.html).

```yaml
name: hello_world
schedule_interval: rate(10 minutes)   # Can also be a cron expression or a string such as "@daily"

nodes:
  send_data:
    function: typhoon.flow_control.branch   # Yields a batch for each item in branches
    config:
      branches:
        - ['users.txt', 'John, Amy, Adam, Jane']
        - ['animals.txt', 'dog, cat, mouse, elephant, giraffe']
        - ['fruits.csv', 'apple,pear,apricot']

  write_data:
    function: typhoon.filesystem.write_data
    config:
      conn_id: $HOOK.data_lake


edges:
  e1:
    source: send_data
    adapter:
      data => APPLY: transformations.data.to_bytes_buffer($BATCH[1])
      path => APPLY: f'/store/{$BATCH[0]}'
    destination: write_data
```

This DAG has three branches, each with a file name and a string and for each branch it will write the string data into that file

!!! info "Skim the code"
    Don't get caught up on the details, we will explain how to create your own DAGs in the following section.

### Running the DAG

We can run this with:

```bash
typhoon dag run --dag-name hello_world
```

We can check the files that have been written to `/tmp/data_lake/store/` where `/tmp/data_lake/` was the base directory defined for our connection (go back to `connections.yml` to check) and the DAG wrote to `/store/[FILE_NAME]`.

The file name was the first item of the list in each batch (eg: the first batch is the list `['users.txt', 'John, Amy, Adam, Jane']`) and the second item of the list are the contents that were written to that file.
