from typing import Optional


def table_list_to_execute_query(
        data: str,
        conn_id: Optional[str] = None,
        schema: Optional[str] = None,
        query: Optional[str] = None,
        batch_size: Optional[int] = None,
        query_template_params: Optional[dict] = None,
) -> dict:
    out = dict(table_name=data)
    if conn_id is not None:
        out.update(conn_id=conn_id)
    if schema is not None:
        out.update(schema=schema)
    if query is not None:
        out.update(query=query)
    if batch_size is not None:
        out.update(batch_size=batch_size)
    if query_template_params is not None:
        out.update(query_template_params=query_template_params)
    return out
