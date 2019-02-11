import logging
from contextlib import closing
from itertools import count
from typing import Optional, NamedTuple, Sequence, Generator

import jinja2

from typhoon.contrib.hooks.dbapi_hooks import DbApiHook
from typhoon.contrib.hooks.hook_factory import get_hook


class ExecuteQueryResult(NamedTuple):
    schema: str
    table_name: str
    columns: Sequence
    batch: Sequence[Sequence]
    batch_num: int


def execute_query(
        conn_id: str,
        schema: str,
        table_name: str,
        query: str,
        batch_size: Optional[int] = None,
        query_template_params: Optional[dict] = None,
) -> Generator[ExecuteQueryResult, None, None]:
    """
    Executes query against a relational database. Schema and table name are returned with the result since they can be
    useful for governance purposes.
    :param conn_id: Should belong to a DbApiHook
    :param schema:  Can be used as template parameter {{ schema }} inside the query
    :param table_name: Can be used as template parameter {{ table }} inside the query
    :param query: Query. Can be a jinja2 template
    :param batch_size: Used as parameter to fetchmany. Full extraction if not defined.
    :param query_template_params: Will used to render the query template
    :return: ExecuteQueryResult namedtuple
    """
    hook: DbApiHook = get_hook(conn_id)
    query_template_params = query_template_params or {}
    query = jinja2.Template(query).render(
        dict(schema=schema, table_name=table_name, **query_template_params)
    )
    with hook as conn, closing(conn.cursor()) as cursor:
        logging.info(f'Executing query: {query}')
        cursor.execute(query)
        columns = [x[0] for x in cursor.description]
        if not batch_size:
            logging.info(f'Fetching all results for {schema}.{table_name}')
            yield ExecuteQueryResult(
                schema=schema,
                table_name=table_name,
                columns=columns,
                batch=cursor.fetchall(),
                batch_num=1,
            )
        else:
            for batch_num in count(start=1):
                logging.info(f'Fetching {batch_size} rows for {schema}.{table_name}')
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                yield ExecuteQueryResult(
                    schema=schema,
                    table_name=table_name,
                    columns=columns,
                    batch=batch,
                    batch_num=batch_num,
                )