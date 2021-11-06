# Functions

Functions are the key building blocks of DAG tasks. Each task has to have a function. Usually functions act on Hooks to extract or load data. 

In the example below we are using a custom function `exchange_rates_api.get_history` that would in the file `exchange_rates_api.py` in the `functions` directory. In this file you would put any function that your custom ExchangeRate Hook might need. 

- `typhoon.function_file.function_name` for built-in / contrib functions.
- `functions.my_function_file.function_name` for accessing your project functions. 

The two main task keys that interact with the function are the `input` (if required) and the `args`. These match the input paramater names of the function. So if you are unsure of how to make the task you can always check the function code. 

For example the write_data function has the following parameters that are matched in the YAML DAG below it. Optional ones are omitted. 

```python
    def write_data(
            data: Union[str, bytes, BytesIO],
            hook: FileSystemHookInterface,
            path: Union[Path, str],
            create_intermediate_dirs: bool = False,
            metadata: Optional[dict] = None,
            return_path_format: str = 'relative',
    ) -> Iterable[str]:
```

### Example built-in / contrib. function
```YAML
  write_csv:
    input: exchange_rate
    function: typhoon.filesystem.write_data
    args:
      hook: !Hook echo
      data: !MultiStep
        - !Py transformations.xr.flatten_response($BATCH)
        - !Py typhoon.data.dicts_to_csv($1, delimiter='|')
      path: !Py f'{$DAG_CONTEXT.interval_start.isoformat()}_xr_data.csv'
```


### Example project function
```YAML
  exchange_rate:
    function: functions.exchange_rates_api.get_history
    args:
      start_at: !Py $DAG_CONTEXT.interval_start
      end_at: !Py $DAG_CONTEXT.interval_end
```