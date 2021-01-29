from typhoon.contrib.functions import relational
from typhoon.contrib.hooks.dbapi_hooks import SnowflakeHook
from typhoon.contrib.schemas.metadata import FieldMetadata
from typhoon.contrib.templates.snowflake import CustomFileFormatTemplate, CopyTemplate, FileFormatType, Quoted, FileFormatCompression


def copy_into(
        hook: SnowflakeHook,
        table: str,
        stage_name: str,
        s3_path: str,
) -> str:
    fields = [FieldMetadata(name='content', type='variant')]
    file_format = CustomFileFormatTemplate(
        type=FileFormatType.JSON,
        field_delimiter=Quoted(','),
        null_if=('null', 'NULL'),
        compression=FileFormatCompression.AUTO,
    )
    audit_fields = 'etl_id number,\netl_timestamp timestamp_ntz\netl_filename'
    query = CopyTemplate(
        table,
        stage_name,
        s3_path,
        fields,
        file_format,
        audit_fields=audit_fields,
    ).render()
    relational.execute_query(hook, schema=hook.conn_params.schema, table_name=table, query=query)
    return table
