# Improving our DAG

In the previous section we created a DAG that reads from a local directory and writes to a SQLite file. We will now improve on that DAG by introducing new concepts that will help us make it more flexible and testable.

## Variables

Variables let us define a value outside our DAG. In general it is a good idea not to hard code magic values into our code, and DAGs in Typhoon are no exception. This makes it easier to change or tune the value without requiring us to re-deploy our DAG.

!!! info "Similar to Airflow's variables"
    If you're familiar with Airflow you'll notice that it's the same concept as Airflow variables. As much as possible Typhoon tries to depart from tried and tested solutions only when it makes . Variables and connections are something that for the most part just works in Airflow, so we saw no reason to change it adding more cognitive burden on developers.
    
    With that said it's worth pointing out the differences. In Typhoon variables have types associated with them. If a variable is of type JSON or YAML it will get loaded into a python object. If you need it to be loaded as a string prefer to choose the TEXT type.

### SQL as variable

Let's move our insert statement into a variable so we can change it in the future if we need to. We'll change the line

```yaml
query: INSERT INTO typhoon_tutorial VALUES (?, ?, ?)
```

for:
```yaml
query: $VARIABLE.tutorial_insert_query
```

We named our variable `insert_query` but we could have given it any name.

!!! tip "Typhoon status" 
    If we run `typhoon status` now we will see the following warning:
    
    ```text
    • Found variables in DAGs that are not defined in the metadata database
       - Variable tutorial_insert_query is not set. Try typhoon variable add --var-id tutorial_insert_query --var-type VAR_TYPE --contents VALUE None
    ```
    
    It's useful to run `typhoon status` often as it will warn us about problems on our project. Lets define the variable now as suggested:

#### Adding the variable

We will also turn the variable into a Jinja template which we will render later.

```bash
typhoon variable add --var-id tutorial_insert_query --var-type jinja --contents "INSERT INTO TABLE typhoon_tutorial VALUES ({{ file_name }}, {{ num_words }}, {{ ts }})"
```

!!! info "Adding variables from STDIN"
    We can add variables from STDIN by leaving out the `--contents` argument. This will prompt us to give the value for the variable. We can also pipe the value UNIX style. Eg: `cat query_file.sql | typhoon variable add --var-id tutorial_insert_query --var-type jinja` or `typhoon variable add --var-id tutorial_insert_query2 --var-type string < query_file.sql`

We can now run `typhoon status` again to see there is no issue, and check our newly created variable  with:

```bash
typhoon variable ls -l
```

!!! info "Web UI DAG Editor"
    The web UI has an experimental DAG editor with intelligent code completion for things like variable and connection names, as well as functions, transformations and node names. However it's sometimes a little behind the CLI and can have some bugs caused by syntax checking. It will be re-written at some point in the future, but in the mean time check it out with `typhoon webserver`. It's really useful when it works correctly.

## Connections

In our functions we hardcoded the logic to read from the filesystem and to write to SQLite. This is not ideal for a few reasons:

- Limited re-usability: Overly specific functions means we are unlikely to find them useful in other DAGs.
- Limited flexibility: If the logic stays the same but the data source changes (eg: Postgres instead of SQLite) we need to modify the function.
- Security: Hard-coding connection details in functions is never a good idea. Passing them as parameters in DAGs is not much better as you probably want to version control your DAGs.

To solve this Typhoon provides Connections which let you define a type (eg: SQLite) and connection parameters. Connections are stored in the database and are used to instantiate hooks. The connection type determines which hook class needs to be instantiated.

Lets now change both the source and destination to use connections.

!!! info "Similarities with Airflow"
    Connections in Typhoon are also very similar to Airflow's connections, down to naming of their parameters and the inclusion of an extra field for parameters that don't fit in the default ones. We depart from Airflow in some ways however:
    
    - In contrast to Airflow it **lets the user define new connection types that are first class** (ie: you get code completion for them in the CLI and you can select them in the UI).
    - It attempts to group similar connections in interfaces/protocols so that they can be swapped for one another, simplifying testing or data source changes. Airflow does this somewhat, for example with DbApiHook, but we take this concept a lot further by grouping other similar data sources such as filesystems (Local Storage, FTP, SFTP, S3 etc can all be used interchangeably). Of course you can always access the underlying connection if you need specific functionality that is not easily re-usable.

### FileSystem Hook

To read from the directory we will use a `FileSystemHook`. Specifically a `LocalStorageHook` though as we'll see this can be changed later. We'll also use a pre-defined Typhoon function called `list_directory` to list files in the directory and another one called `read_data` to read data from these files.

#### Changing the ls node

We need to change the line

```yaml
  ls:
    function: functions.tutorial.get_files_in_directory
    config:
      directory: /tmp/typhoon/landing/
```

to:
```yaml
  ls:
    function: typhoon.filesystem.list_directory
    config:
      hook: $HOOK.tutorial_fs
      path: landing
```

Notice how we no longer define the full path in the DAG. FileSystem hooks have a base path that gets prepended to it (eg: In the S3 hook that's the bucket, in an FTP hook you might want a different base directory for testing and another for production etc.).

#### Adding the hook

We will add the hook to our `connections.yml` file:

```yaml
tutorial_fs:
  local:
    conn_type: local_storage
    extra:
      base_path: /tmp/typhoon/
```

Then add it to our metadata database:

```bash
typhoon connection add --conn-id tutorial_fs --conn-env local
```

#### Reading the files

We will add an additional node to read the file contents using another predefined Typhoon function:

```yaml
read_file:
  function: typhoon.filesystem.read_data
  config:
    hook: $hook.tutorial_fs
```

### Changing the count_words node and function

We will change our function to receive a text and just count the words, removing the file reading logic from it. It is generally a good idea for a function to do one and only one thing. We will also take a dict of metadata so we can keep track of where the text came from.

```python
T = TypeVar('T')

def words_in_text(text: str, metadata: T) -> Tuple[int, T]:
    num_words = len(re.split('\s+', text))
    return num_words, metadata
```

```yaml
count_words:
  function: functions.tutorial.words_in_text
```

### DB-API Hook

We will continue making our DAG more generic by using the Typhoon function execute_query, which takes a `DbApiHook`, which can be any hook supporting the [DB-API interface](https://www.python.org/dev/peps/pep-0249/). Almost every database library in Python supports it, so we will be able to use them interchangeably (if the SQL query is valid in both databases).

#### Changing the write_to_db node

We will change

```yaml
  write_to_db:
    function: functions.tutorial.execute_on_sqlite
    config:
      sqlite_db: /tmp/typhoon/tutorial.db
      query: $VARIABLE.tutorial_insert_query
```

to:

```yaml
  write_to_db:
    function: typhoon.relational.execute_query
    config:
      hook: $HOOK.tutorial_db
      query: $VARIABLE.tutorial_insert_query
```

#### Adding the hook

First we define the connection details in the `connections.yml` file:

```yaml
tutorial_db:
  dev:
    conn_type: sqlite
    extra:
      database: /tmp/typhoon/tutorial.db
```

Then we add the connection with the CLI:

```bash
typhoon connection add --conn-id tutorial_db --conn-env local
```

### Edges: putting it all together

Finally to make all of this come together we need toWe need to change the edges. The final DAG will be:

```yaml
name: tutorial
schedule_interval: "@hourly"

nodes:
  ls:
    function: typhoon.filesystem.list_directory
    config:
      hook: $HOOK.tutorial_fs
      path: landing

  read_file:
    function: typhoon.filesystem.read_data
    config:
      hook: $hook.tutorial_fs
  
  count_words:
    function: functions.tutorial.words_in_text
  
  write_to_db:
    function: typhoon.relational.execute_query
    config:
      hook: $HOOK.tutorial_db
      query: $VARIABLE.tutorial_insert_query
      
edges:
  e1:
    source: ls
    adapter:
      file_path => APPLY: $BATCH
    destination: count_words
    metadata:
      file_path => APPLY: $ADAPTER.file_path
    
  e2:
    source: count_words
    adapter:
      parameters => APPLY: "[$BATCH[0], $BATCH_METADATA.file_path, $DAG_CONTEXT.execution_date.isoformat()]"
    destination: write_to_db
```
