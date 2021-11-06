# Simple Pandas Examples

Using Pandas means we get access to a hugely powerful and well known library. It's simply the easiest way to transform data many complex tasks:

- time-series analysis 
- complex merging
- cleaning
- pivoting / melting etc. 

Here is a very trivial example to illustrate two ways you can access Pandas in a DAG. From this you can integrate existing code or quickly add Pandas into your automations.  

## Direct usage in DAG YAML 

It's very easy to use Pandas directly in !MultiStep transformations without even creating a function. 

Here we have loaded a mock data set of test scores (attached) and created a small function to load the json. 


```yaml
name: pandas_example
schedule_interval: rate(1 hour)

tasks:
  load_json:
    function: typhoon.filesystem.read_data
    args:
      hook: !Hook data_lake
      path: /test_scores.json


  pandas_transform:
    input: load_json
    function: typhoon.debug.echo
    args:
      data: !MultiStep
        - !Py transformations.data.json_loads_to_dict($BATCH.data)
        - !Py typhoon.data.to_dataframe($1)
        - !Py $2.groupby('Country').mean()['Score']
        - !Py $2.merge($3, how='inner', on='Country')
        - !Py $4.rename(columns={"Score_x":"Score","Score_y":"Country_median_score"})
```

The simple function to load the json:
```python
def json_loads_to_dict(data: Union[str, bytes]) -> dict:
    import json
    return json.loads(data)
```

Data set - mock test scores 

## Usage in functions

We can also wrap this into a function:

```python
def df_add_country_median(data: pd.DataFrame) -> pd.DataFrame:
    df = data.groupby('Country').mean()['Score']
    data = data.merge(df, how = 'inner', on = 'Country')
    return data.rename(columns={"Score_x": "Score", "Score_y": "Country_median_score"})
```

This can then simplify the DAG:
```yaml
  pandas_transform:
    input: load_json
    function: typhoon.debug.echo
    args:
      data: !MultiStep
        - !Py transformations.data.json_loads_to_dict($BATCH.data)
        - !Py typhoon.data.to_dataframe($1)
        - !Py transformations.data.df_add_country_median($2)
```