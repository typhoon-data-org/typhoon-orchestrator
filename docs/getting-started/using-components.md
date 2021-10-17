# Using Components

## What is a component?

Components are ways of packaging up sets of regularly used tasks. This encourages modularity and re-use. 

This means a library of powerful workflows that are then simple dropping in  2-3 line YAML fragments. 

Components can be 'in-flow' using previous steps as inputs. Alternatively, 'standalone' ones can be built as DAG full templates. ***'Standalone' components can use the Component Builder UI*** 

Example components:

- *in-flow*: if, glob_complress_and_copy, signer_demultiplexer
- *standalone*: db_to_snowflake, csv_my_pandas_transform        

 
 @TODO - screenshot of UI

## In-flow component usage

@ TOdo - usage of if and glob_compress

## Standalone Component usage

@ Todo  -usage of a simple component of transform

@ Todo usage of Snowflake in code