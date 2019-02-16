# Typhoon Orchestrator

![alt text](https://cdn.pixabay.com/photo/2015/07/27/19/49/beach-863346_1280.jpg)

Typhoon is a task orchestrator and workfow manager used to create asynchronous data pipelines that can be deployed to AWS Lambda/Fargate to be completely serverless.

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
def write_data(data: BytesIO, conn_id: str, path: str) -> Iterable[str]:
    hook: FileSystemHookInterface = get_hook(conn_id)
    with hook:
        hook.write_data(data, path)
    yield path
```

The above code is all that's needed to create a function that can be used to define a task, and is a great example of what we discussed earlier. This code is pure python, and has no Typhoon specific code except for get_hook() (we believe that in this case the benefit it provides is well worth the extra coupling, but it is by no means necessary to write your functions in the same way) and even that can be imported to a non Typhoon project (yes, even Airflow) by doing `pip install typhoon[hooks]` if you ever wish to move away from the framework. Notice how this is inherently more testable and reusable than the alternative approach.

We mentioned earlier that OOP makes sense for hooks. In this case by having a FileSystem Hook interface, both S3, FTP and local writes to disk amongst others can be performed in the same way by calling write_data. This brings an extra layer of testability where you can simply write data to disk during development/debugging and then deploy to a test or production server where it will be written to S3 without any code modifications, simply by virtue of having the conn_id point to an S3 connection in that environment.

A Task is defined in a DAG by referencing that function and passing it the static arguments (the rest are passed in runtime by the preceding Task). From now on we will call them nodes instead of tasks. We will later justify our decision of using YAML instead of python for DAG definitions.

```yaml
  write_data:
    function: typhoon.filesystem.write_data     # Previously defined function
    config:
      conn_id: s3_data_lake     # This can point to a Local Storage Hook in dev if you wish
```

Notice how this function can be used in any DAG that needs to write to S3, no matter the source of the data. Contrast that to airflow where in [contrib/operators/](https://github.com/apache/airflow/tree/master/airflow/operators) we have a hive_to_druid operator, hive_to_mysql, hive_to_samba, mssql_to_hive, mysql_to_hive and so on. You get the idea of the NxM complexity of defining operators that we discussed earlier.

### Asynchrony


Factories                             |  Assembly Line
:------------------------------------:|:------------------------------------:
![](https://i.imgur.com/jsfwxeE.png)  |  ![](https://i.imgur.com/ycWPMmJ.png)


To illustrate the limitations of the prevalent approach to data pipelines, picture a car assembly line. Each plant is specialised in one thing and one thing only, and from a series of raw materials it builds a product that the next piece in the chain can take, transform and assemble into something else. If we ever want to change the body of the car we can still reuse the engine, suspensions and a lot of the pieces.

Now imagine if each factory was isolated and there was no communication between them. The engine factory would produce all the engines for the day, then leave them at a specific place that had to be agreed beforehand. Once all the engines are produced, the assembly plant, which has been sitting idle while all the engines piled up, would then go get them a that specific location, bring them over and start assembling them into the power trains, then leave them in a different pre agreed location. Once all the power trains are created, the next piece in the assembly line has to get them and assemble them into the body, and so on. You can see how terribly inefficient that process would be. Also note how every piece has to be aware of the other and know where it needs to go to fetch the product (data) because there is no communication between them. Paradoxically, task isolation has actually created coupling. This is in a nutshell how current workflow managers work.

A better way would be to push the engines out of the factory as soon as each one of them is finished. We do not need to care about what happens to them after. Maybe a truck will pick it up. Or a conveyor belt will carry it. We just know that it will get delivered where it's needed and that is someone else's responsibility. On the other side, another factory wil get fed that engine and start using it along with all the other parts to build the first car, all the while most of the parts for future cars are being produced at the same time.

The product (analogous to our data) flows from one factory to the next, improving construction time dramatically while at the same time simplifying orchestration by reducing the scope and responsibility of each piece. This is what Typhoon strives to achieve.
