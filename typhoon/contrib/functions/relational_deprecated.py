import logging
from contextlib import closing
from typing import Optional, NamedTuple, Sequence, Generator, Iterable

import sqlparse

from typhoon.contrib.hooks.dbapi_hooks import DbApiHook
from typhoon.contrib.hooks.sqlalchemy_hook import SqlAlchemyHook


class ExecuteQueryResult(NamedTuple):
    metadata: dict
    columns: Sequence
    batch: Sequence[Sequence]


def execute_query(
        hook: DbApiHook,
        query: str,
        batch_size: Optional[int] = None,
        metadata: Optional[dict] = None,
        query_params: Optional[dict] = None,
        multi_query: bool = False,
) -> Generator[ExecuteQueryResult, None, None]:
    """
    Executes query against a relational database. Schema and table name are returned with the result since they can be
    useful for governance purposes.
    :param hook: DbApiHook instance
    :param schema:  Can be used as template parameter {{ schema }} inside the query
    :param table_name: Can be used as template parameter {{ table }} inside the query
    :param query: Query. Can be a jinja2 template
    :param batch_size: Used as parameter to fetchmany. Full extraction if not defined.
    :param query_params: Will used to render the query template
    :return: ExecuteQueryResult namedtuple
    """
    with hook as conn:
        for single_query in (sqlparse.split(query) if multi_query else [query]):
            logging.info(f'Executing query: {single_query}')
            if isinstance(hook, SqlAlchemyHook):
                cursor = conn.engine.execute(single_query, query_params)
                columns = [x[0] for x in cursor._cursor_description()]
            else:
                cursor = conn.cursor()
                cursor.execute(single_query, query_params)
                columns = [x[0] for x in cursor.description] if cursor.description else None
            
            # @todo put connection to autocommit=True
            #conn.commit()

            if not batch_size:
                logging.info(f'Fetching all results')
                yield ExecuteQueryResult(
                    metadata=metadata,
                    columns=columns,
                    batch=cursor.fetchall(),
                )
            else:
                while True:
                    logging.info(f'Fetching {batch_size} rows')
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    yield ExecuteQueryResult(
                        metadata=metadata,
                        columns=columns,
                        batch=batch,
                    )


def read_sql(hook: DbApiHook, query: str, batch_size: Optional[int] = None, metadata: dict = None):
    import pandas as pd
    with hook as conn:
        if batch_size is None:
            return pd.read_sql(query, conn), metadata
        for chunk in pd.read_sql(query, conn, chunksize=batch_size):
            yield chunk, metadata


def execute_many(hook: DbApiHook, query: str, seq_of_params: Iterable[tuple], metadata: dict = None) -> dict:
    with hook as conn, closing(conn.cursor()) as cursor:
        cursor.executemany(query, seq_of_params)
    return metadata


# def df_write(df: DataFrame, hook: SqlAlchemyHook, table_name: str, schema: str = None):
#     """
#     Given conn_id belonging to a SqlAlchemy hook, create or append the data to the specified table
#     :param df: Dataframe with data
#     :param hook: SqlAlchemyHook instance
#     :param table_name: Name of the table to write to
#     :param schema: Schema where the table is located
#     :return:
#     """
#     with hook as engine:
#         logging.info(f'Writing dataframe to {hook.conn_params.conn_type} table {table_name}, schema {schema or "default"}')
#         df.to_sql(name=table_name, con=engine, schema=schema, if_exists='append')
#     return schema, table_name
