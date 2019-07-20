# Typhoon Orchestrator

![alt text](https://cdn.pixabay.com/photo/2015/07/27/19/49/beach-863346_1280.jpg)

Typhoon is a task orchestrator and workfow manager used to create asynchronous data pipelines that can be deployed to AWS Lambda/Fargate to be completely serverless.

**This project is in pre-alpha and subject to fast changes**

## Principles

Typhoon is inspired by Airflow but departs from it in key ways. The most important being that unlike Airflow where tasks are isolated, data is passed from parent node to child nodes. Another important consideration is that the data is passed in batches with the option to process each batch in a new lambda instance for unlimited parallelism. Throughout this document we will constantly refer to Airflow, not because we are trying to criticise it, but because it is the tool to beat at the moment and as such the best measuring stick there is. We have used Airflow to great success and drew heavily from it in some ways.

### Data sharing and composability
 
 The ability to share data also has important implications for composability and code reuse. In Airflow you would have an operator that reads from one source and writes to another (eg: reads from Postgres and writes to S3). Whenever you want to read from Postgres and write to an FTP, or read from Mongo and write to S3 you need to create a different operator. This means that for N sources and M destinations you need to write potentially NxM Operators, repeating a lot of the code. In contrast, Typhoon would have a function to extract from Postgres (or any other DbAPI compatible connection), another for MongoDB and so on for every source. We would also have a function for every destination and adapt the output of the source to the input of the destination function via transformations.
 
 This way we just have N+M functions to maintain and can create new workflows by simply composing them and adapting the output of one to the input of the next.
 
 ### Functional Data Pipelines
 
 ![alt text](https://i.imgur.com/AoE9UuE.png)
 
 In the [following article](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) (well worth a read), Airflow's creator Maxime Beauchemin advocates for functional data engineering and does a great job of laying out guidelines for doing just that, advocating "pure" and "immutable" tasks (by an admittedly loose definition of the terms, after all functions can't easily be pure when dealing with external sources). While this can be of great value and adds a lot of clarity to your processes, Airflow is still Object Oriented at heart. That coupled with the fact that Operators are isolated leads to a lot of Airflow-specific code that mixes implementation, business logic and regular data processing logic. Apart from making it harder to reuse code, this also makes it difficult to migrate it to a different technology.
 
 With Typhoon we share the same philosophy outlined in that article and take it a step further. We believe that while Object Oriented Programming certainly has its value (and we use it where it makes sense, like implementing Hooks for instance) a functional approach brings greater value for data processing where we just care about manipulating data and have no desire to abstract from it in any way. Where you would define an operator class in Airflow, this is replaced directly with functions in Typhoon.
 
 ```python
def write_data(data: BytesIO, hook: FileSystemHookInterface, path: str) -> Iterable[str]:
    with hook:
        hook.write_data(data, path)
    yield path
```

The above code is all that's needed to create a function that can be used to define a task, and is a great example of what we discussed earlier. This code is pure python, and has no Typhoon specific code. Notice how this is inherently more testable and reusable than the alternative approach.

We mentioned earlier that OOP makes sense for hooks. In this case by having a FileSystem Hook interface, both S3, FTP and local writes to disk amongst others can be performed in the same way by calling write_data. This brings an extra layer of testability where you can simply write data to disk during development/debugging and then deploy to a test or production server where it will be written to S3 without any code modifications, simply by virtue of having the conn_id point to an S3 connection in that environment.

A Task is defined in a DAG by referencing that function and passing it the static arguments (the rest are passed in runtime by the preceding Task as we will see in the Edges section). From now on we will call them nodes instead of tasks. We will later justify our decision of using YAML instead of python for DAG definitions.

```yaml
  write_data:
    function: typhoon.filesystem.write_data     # Previously defined function
    config:
      hook: $HOOK.s3_data_lake     # This can point to a Local Storage Hook in dev if you wish
```

Notice how this function can be used in any DAG that needs to write to S3, no matter the source of the data. Contrast that to airflow where in [contrib/operators/](https://github.com/apache/airflow/tree/master/airflow/operators) we have a hive_to_druid operator, hive_to_mysql, hive_to_samba, mssql_to_hive, mysql_to_hive and so on. You get the idea of the NxM complexity of defining operators that we discussed earlier.

### Asynchrony


Factories                             |  Assembly Line
:------------------------------------:|:------------------------------------:
![](https://i.imgur.com/jsfwxeE.png)  |  ![](https://i.imgur.com/ycWPMmJ.png)


To illustrate the limitations of the prevalent approach to data pipelines, picture a car assembly line. Each plant is specialised in one thing and one thing only, and from a series of raw materials it builds a product that the next piece in the chain can take, transform and assemble into something else. If we ever want to change the body of the car we can still reuse the engine, suspensions and a lot of the pieces.

Now imagine if each factory was isolated and there was no communication between them. The engine factory would produce all engines for the day, then leave them at a specific place that had to be agreed beforehand. Once all the engines are produced, the assembly plant, which has been sitting idle while all the engines piled up, would then go get them a that specific location, bring them over and start assembling them into the power trains, then leave them in a different pre agreed location. Once all the power trains are created, the next piece in the assembly line has to get them and assemble them into the body, and so on. You can see how inefficient that process would be. Also note how every piece has to be aware of the other and know where it needs to go to fetch the product because there is no communication between them. Paradoxically, task isolation has actually created coupling. This is in a nutshell how most current workflow managers work.

A better way would be to push the engines out of the factory as soon as each one of them is finished. We do not need to care about what happens to them after. Maybe a truck will pick it up. Or a conveyor belt will carry it. We just know that it will get delivered where it's needed and that is someone else's responsibility. On the other side, another factory wil get fed that engine without caring where it came from and start using it along with all the other parts to build the first car, all the while most of the parts for future cars are being produced at the same time.

The product (analogous to our data) flows from one factory to the next, improving construction time dramatically while at the same time simplifying orchestration by reducing the scope and responsibility of each piece. This is what Typhoon strives to achieve. Each batch is pushed to the next node to be processed in a different Lambda while the current node immediately starts working on the next batch.

![alt text 1](https://i.imgur.com/tgTF6bv.png)

On the image we see a simple DAG definition with two nodes. At execution time the source function will call the child function with some batch of data. The child function will run in a different instance of Lambda/Fargate to the parent function (and also a different one per batch) so the parent function is free to continue its execution. We can see that each instance of the child node can also create batches and send them to its child nodes achieving complete parallelism. We do not need to wait for preceding tasks to finish to execute their child tasks, the whole workflow can be executing simultaneously as the data becomes available.

### Edges: Connecting nodes together

We have seen how to define functions and nodes, but how do they fit together? How do we adapt the output of one to the input of the other We connect them together by defining edges (keeping with the graph terminology) in YAML and use transformation functions to adapt input to output.

Assume we have two nodes: send_data and write_data. Send_data sends batches made up of a tuple with (file_name, string_data), and write_data takes a BytesIO buffer in a parameter named data and the path where it has to be written to in a parameter named path. The edge would be defined as follows:

```yaml
edges:
  e1:
    source: send_data
    adapter:
      data => APPLY: transformations.data.to_bytes_buffer($BATCH[1])
      path => APPLY: f'/tmp/{$BATCH[0]}'
    destination: write_data
```

Notice how an edge is made up of a source node, a destination node and an adapter. The adapter has to define all the necessary parameters to the destination_function by applying transformations to the source function output. This is all standard YAML except for two things:

- `=> APPLY` is used to indicate that the value received is not a string, but some code that needs to be executed at runtime to produce a value.
- `$BATCH` is one of a limited set of special variables (all in uppercase and starting with $) that will be substituted during code generation. This one in particular refers to the output (batch) of the source function.

This part will be used to generate python code roughly equivalent to:

```python
source_data = send_data_node()
for source_data_batch in source_data:
    destination_config = {
        'data': transformations.data.to_bytes_buffer(source_data_batch[1]),
        'path': f'/tmp/source_data_batch[0]'
    }
    write_data(
        # every static parameter defined in the write_data node config. eg: conn_id='s3_data_lake',
        ** destination_config
    )
```

We recognize that some transformations can be complex, or too long to comfortably fit in a line, so an  `=> APPLY` key can take a list as a value, where the final value will be the result of executing the last line in the list. Previous transformations in the list can be referred to by index with `$N` where N is the position in the list. This is clearer with an example. Take the same function but lets assume it returns a named tuple instead of a regular tuple for clarity, and it can optionally return an encoding to apply:

```yaml
edges:
  e1:
    source: send_data
    adapter:
      data => APPLY:
        - f'HEADER\n{$BATCH.string_data\nFOOTER}'
        - $BATCH.encoding or 'utf-8'
        - transformations.data.to_bytes_buffer(data=$1, encoding=$2)
      path => APPLY: f'/tmp/{$BATCH.file_name}'
    destination: write_data
```

The code generated this time would be roughly:

```python
source_data = send_data_node()
for source_data_batch in source_data:
    destination_config = {}
    data_1 = f'HEADER\n{source_data_batch.string_data}\nFOOTER'
    data_2 = source_data_batch.encoding or 'utf-8'
    destination_config['data'] = transformations.data.to_bytes_buffer(data=data_1, encoding=data_2)
    destination_config['path'] = f'/tmp/{source_data_batch.file_name}'
    write_data(
        # every static parameter defined in the write_data node config. eg: conn_id='s3_data_lake',
        ** destination_config
    )
```

## Putting it all together

Just so you get an idea of how it all fits together lets suppose we have the following pieces already developed or available in Typhoon:

- `typhoon.flow_control.branch`: Which is a function that takes a list and yields each element.
- `typhoon.relational.execute_query`: Function that takes a query, a table name and a batch size (indicates how many rows to fetch), executes this query and returns the rows, the columns, a batch result and the table name (the table name is not used in the extraction but it is passed nontheless because it can be useful further up to, for example, make up the S3 key).
- `typhoon.filesystem.write_data`: Takes a bytes object and a path where it writes the data.
- `typhoon.templates.render`: Transformation that takes a jinja template as a string and keyword arguments that will be used to render it.
- `typhoon.db_result.to_csv`: Transformation function that takes a query execution result and transforms it into a CSV string.

And we want to create a workflow that executes every day at 9pm, extracts data from 3 tables named `person`, `job`, and `property` from a database, then writes it in CSV form to S3. The definition for this DAG might be as follows:

```yaml
name: example_dag
schedule_interval: '0 21 * * *'  # Run every day at 21:00

nodes:
  send_table_names:
    function: typhoon.flow_control.branch
    config:
      branches:
        - person
        - job
        - property

  extract_table:
    function: typhoon.relational.execute_query
    config:
      hook: $HOOK.test_db

  load_csv_s3:
    function: typhoon.filesystem.write_data
    config:
      hook: $HOOK.s3_data_lake


edges:
  send_extraction_params:
    source: send_table_names
    adapter:
      table_name => APPLY: $BATCH
      query => APPLY:
        - str("SELECT * FROM {{ table_name }} WHERE creation_date='{{ date_string }}'")
        - typhoon.templates.render(template=$1, table_name=$BATCH, date_string=$dag_context.ds)
      batch_size: 200
    destination: extract_table

  table_to_s3:
    async: false        # The table may be large, it doesn't make sense to serialize each batch and send asynchronously
    source: extract_table
    adapter:
      data => APPLY:
        - typhoon.db_result.to_csv(description=$BATCH.columns, data=$BATCH.batch)
        - $1.encode('utf_8')
      path => APPLY:
        - str('{{system}}/{{entity}}/{{dag_cfg.ds}}/{{dag_cfg.etl_timestamp}}_{{part}}.{{ext}}')
        - typhoon.templates.render($1, system='postgres', entity=$BATCH.table_name, dag_cfg=$dag_context, part=$BATCH_NUM, ext='csv')
    destination: load_csv_s3
```

In a nutshell, branch will launch three lambdas, one for each table name and they will all extract data in batches simultaneously and send each batch to S3. Since batches can be very large it is not feasible to send it over to a new lambda function instance, so we set `async: false` to force it to execute the database extraction and S3 upload in the same lambda function. This is a tradeoff that reduces parallelism but keeps data transfer low. This way you don't need to blend them into one node to prevent it from lauching a new Lambda instance and you can still define them as regular nodes with the same composability this provides. If you still want to increase parallelism you can use `async: thread` and each instance of `load_csv_s3_node` will be run in a new thread (good for performance if there's a lot of IO). Realize that this is only for nodes that need to share a large amount of data that can't be easily serialized, if we wanted to import that data from S3 into our warehouse we just need to pass it the s3 key so that can be run in a regular asynchronous node that will trigger a new Lambda function instance.

Those who are familiar will recognize `$dag_context` as being very similar to the context received by Operators in Airflow, with the difference that in Typhoon we try to make our node functions unaware of such low level details and handle DAG specific configuration in the DAG definition where it belongs.

The `$HOOK` special variable creates a hook with the connection id passed after the '.' with the right connection type by calling typhoon's connection factory (which can be extended with custom hooks).

Finally `$BATCH_NUM` is the last of the available special variables which denotes the current batch number, which is the same as the amount of batches that the source function has produced up to this point



## More coming soon...
