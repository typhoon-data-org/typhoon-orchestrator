# Typhoon Orchestrator

![alt text](https://cdn.pixabay.com/photo/2015/07/27/19/49/beach-863346_1280.jpg)

Typhoon is a task orchestrator and workfow manager used to create asynchronous data pipelines that can be deployed to AWS Lambda/Fargate to be completely serverless.

## Principles

Typhoon is inspired by Airflow but departs from it in key ways. The most important being that unlike Airflow where tasks are isolated, data is passed from parent node to child nodes. Another important consideration is that the data is passed in batches with the option to process each batch in a new lambda instance for unlimited parallelism.

### Data sharing and composability
 
 The ability to share data also has important implications for composability and code reuse. In Airflow you would have an operator that reads from one source and writes to another (eg: reads from Postgres and writes to S3). Whenever you want to read from Postgres and write to an FTP, or read from Mongo and write to S3 you need to create a different operator. This means that for N sources and M destinations you need to write potentially NxM Operators, repeating a lot of the code. In contrast, Typhoon would have a function to extract from Postgres (or any other DbAPI compatible connection), another for MongoDB and so on for every source. We would also have a function for every destination and adapt the output of the source to the input of the destination function via transformations.
 
 This way we just have N+M functions to maintain and can create new workflows by simply composing them and adapting the output of one to the input of the next.
