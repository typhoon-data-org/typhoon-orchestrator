from pathlib import Path
from typing import Iterable, NamedTuple, Union, List, Optional
from typhoon.contrib.hooks.filesystem_hooks import FileSystemHookInterface
from typhoon.contrib.hooks.sqlalchemy_hook import SqlAlchemyHook
from pandas import DataFrame


def csv_to_df(hook: FileSystemHookInterface, path: Union[Path, str], sep=',', skiprows=None) -> DataFrame:
    """
    Reads the data from a file given its relative path and returns a named tuple with the shape (DataFrame: df, path: str)
    :param hook: FileSystem Hook
    :param path: File path relative to base directory
    :param sep: Default is ',' but you can pass '\t' for example
    :param skiprows: Skip the first row of the CSV?
    :return:
    """
    import pandas as pd
    with hook as conn:
        print(f'Reading from {path}')
        df = pd.read_csv(conn.root_path + str(path), skiprows=skiprows, sep=sep)
        return (df, path)


def df_write(df: DataFrame, hook: SqlAlchemyHook, table_name: str, schema: str = None):
    """
    Given conn_id belonging to a SqlAlchemy hook, create or append the data to the specified table
    :param df: Dataframe with data
    :param hook: SqlAlchemyHook instance
    :param table_name: Name of the table to write to
    :param schema: Schema where the table is located
    :return:
    """
    import pandas as pd
    print(hook, type(hook))

    with hook as engine:
        print(f'Writing dataframe to {hook.conn_params.conn_type} table {table_name}, schema {schema or "default"}')

        # Pandas To_sql must use named col inserts and we don't need to pass them. So get them and assign.
        db_tab_cols = pd.read_sql(f"select * from {table_name} where 1=2", engine) \
            .columns.tolist()

        print("DF Cols", df.columns)
        print("SQL table cols", db_tab_cols)
        df.columns = db_tab_cols
        df.to_sql(name=table_name, con=engine, schema=schema, method='multi', if_exists='append', index=False)

    return schema, table_name
