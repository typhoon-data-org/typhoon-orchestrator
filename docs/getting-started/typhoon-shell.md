# Typhoon Shell

The Interactive Shell is really useful for running tasks and understanding the data structure at each point. Here is a short demo of running the get_author task and seeing the data it returns which can then be explored.

<figure markdown> 
   ![Shell](https://raw.githubusercontent.com/typhoon-data-org/typhoon-orchestrator/feature/docs_gitpages/docs/img/shell_example.gif){ width="800" }
   <figcaption>Inspired by others; instantly familiar.</figcaption>
</figure>


## First usage & help

To run the interactive Shell use `typhoon shell`. For example:

` typhoon shell --dag-name favorite_authors`

**Note** it must be run with the name of the DAG you want to use.


## Key Shell usage:

- **Reload the DAG if you have altered it**:   ```%reload_dag```

- **Autocomplete list the Tasks**:   ```tasks.``` and tab to bring up tasks.

- **Getting args of a task**
  
    - run ```tasks.get_author.get_args(dag_context, None, None, batch="test")```
    - results in ```{'requested_author': 'test'}``` which is the arguement this task needs
  
- **Running a task**:   
  
    - run ```tasks.get_author.run(dag_context, None, None, "J.K. Rowling")```
    - results in returning the data from API.
  
- **Inspecting the data**:   
  
    - run ```tasks.get_author.broker.batches.keys()``` to get the batch key.
    - run ``` data = tasks.get_author.broker.batches['a4dd9048-4d0a-4b74-8b44-338608ddec47']```
    - then you can review ```data```.
  
Note: **dag_context** object exists so you can use it to run tasks easily:
  ```tasks.get_author.run(dag_context, None, None, "J.K. Rowling")```




  

  

