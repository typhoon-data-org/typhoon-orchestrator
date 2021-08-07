# Hooks - Connection interfaces

Typhoon hooks represent the same thing as [airlflow hooks][1], so if you are familiar it's an easy concept. They are the interface to external platforms and databases.

 - About adding existing hooks, see [Adding connections][2].
 - Full list of existing hooks, see [Usage: Hooks][3]  

[1]:https://airflow.incubator.apache.org/docs/apache-airflow/2.0.0/concepts.html?highlight=hooks
[2]:connections.html
[3]:/usage/hooks.html


## Why create a new Hook? 

You may want to access data sources or APIs that don't exist already in any of: 
 - Core or Community contributed [Typhoon Hooks][3]  
    - File system, AWS, Snowflake
 - [SQLAlchemy][5] / [Python DB API 2.0][4] allowing hundreds of DBs without much custom code.
 - [Singer taps][2]. You can use any singer taps in your DAG as a task.   


[4]:https://www.python.org/dev/peps/pep-0249/
[5]:https://docs.sqlalchemy.org/en/14/dialects/


# Creating New Hooks

If you need to make a new one its relatively straightforward. Let's make an example to get data from elasticsearch. You can use [Docker and Elastic][6] to follow along.




[6]:https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html#run-elasticsearch