# Getting started

To install a development build do `pip install typhoon-orchestrator[dev]`. You almost always want the DEV version when installing it locally. If you installed the basic version you'd get the lightweight version that runs in production but doesn't have a lot of the tools that make development easier because they increase the size of the distribution.

## New project

Inside your terminal run:

```bash
typhoon init
```

This will create a directory named hello_world that serves as an example project. Let's change directory into it `cd hello_world`.

## Setting the project home

As in git, one of the most important commands is status. It gives us information about the state of the project as well as tips on how to fix problems or warnings.

```bash
typhoon status dev
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

• Typhoon home not set... To define in current directory run To define in current directory run export TYPHOON_HOME=$(pwd)
Aborting checks...
```

We can see that it's telling us that `TYPHOON_HOME` is not set and recommending us a fix. Let's set it to hello_world by doing

```bash
 export TYPHOON_HOME=$(pwd)
```

!!! tip
    You can add the export (using an absolute path to the project) to `~/.bashrc` or `~/.bash_profile` if you want your terminal to remember the value of this environment variable after you restart it.

!!! warning
    Make sure you are in the hello_world directory when you run the export.

## Edit DAG

We notice that there is an example DAG in `dags/hello_world.yml`.

!!! tip
    To get some editor completion and hints you can configure the JSON schema in `dag_schema.json` as the schema for the DAG YAML file. This can be done in VS Code with the [Red Hat YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) or in [PyCharm natively](https://www.jetbrains.com/help/pycharm/json.html).

```yaml
name: hello_world
schedule_interval: rate(10 minutes)

nodes:
  send_data:
    function: typhoon.flow_control.branch
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
