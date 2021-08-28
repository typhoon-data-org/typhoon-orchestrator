# Hooks

## Hook Interface & Parameters

The minimum you must specify for all conecctions in your `connections.yml` file is the `conn_type`:

```YAML
    conn_type: echo
```

Most Hooks have parameters as well. The default ones which are common in the DB hooks:

```YAML
conn:
  conn_type: mysql
  login: my_login
  password: my_pass
  host: localhost
  port: 3306
```

In addition, many have `extra` parameters which you can list as follows: 
```YAML
data_lake:
  test:
    conn_type: s3
    extra:
      bucket: my-typhoon-test-bucket
```

## Existing Hooks

### Debug
- EchoHook - This simply echo's to the prompt what would be written to the file system. 

### File system

#### LocalStorageHook 

- Local file system / disk. Wrapper around [PyFileSystem][6]
- FTPHook
- S3Hook
- GCSHook

```YAML
example_file_system_hooks:
  local_storage:
    conn_type: local_storage
    extra:
      base_path: /tmp/data_lake/
      create: True 
  
  s3_hook:
    conn_type: s3
    extra:
      bucket: my-typhoon-test-bucket
```


### Http 

- Basic Authentication (URL)

### AWS hooks
- AwsSessionHook
- DynamoDbHook
    
###  DB API 2.0
Hooks and functions related to databases compatible with the [Python Database API Specification v2.0][1].

You need to install the [Typhoon DbAPI][2] contrib package with:

    pip install typhoon-dbapi

- EchoDb (default install)
- SQLite (default install)
- Snowflake: pip install typhoon-dbapi[snowflake]
- Big Query: pip install typhoon-dbapi[bigquery]
- Postgres: pip install typhoon-dbapi[postgres]
- DuckDB: pip install typhoon-dbapi[duckdb]
- MySQL: pip install typhoon-dbapi[mysql]

Any other DB that complies with this DBAPI 2.0 is very simple to add with minor customisation. Should cover vast majority of relational DBs.  

### SQL Alchemy

[SQL Alchemy][3] supports anything with DB API 2.0. 

### Singer

[Singer taps][4] can be used in tasks expanding the range of sources enourmously.

### Kafka

- KafkaConsumerHook
- KafkaProducerHook

### ElasticSearch

[See tutorial][5] for quickly creating your own in less than 10 mins.



[1]:https://www.python.org/dev/peps/pep-0249/
[2]:https://github.com/typhoon-data-org/typhoon-dbapi
[3]:https://www.sqlalchemy.org/features.html
[4]:https://www.singer.io/#taps
[5]:/getting-started/creating-hooks.html
[6]:https://docs.pyfilesystem.org/en/latest/reference/osfs.html?highlight=OSFS#fs.osfs.OSFS