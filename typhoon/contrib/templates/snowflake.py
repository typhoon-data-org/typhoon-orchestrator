from enum import Enum
from typing import List, Union, TypeVar, Generic, Tuple

from dataclasses import dataclass

from typhoon.contrib.schemas.metadata import FieldMetadata
from typhoon.core.templated import Templated


@dataclass
class CopyTemplate(Templated):
    template = """
    COPY INTO {{ table }} from (
        SELECT
        {% for field in fields %}
            ${{ loop.index }} as {{ field.name }}{{ ',' if not loop.last else '' }}
        {% endfor %}
        {% when audit_fields %}{{ audit_fields }}{% endwhen %}
        FROM @{{stage_name}}/{{ s3_path }}
    )
    FILE_FORMAT = {{ file_format }}
    {% when copy_options %}
    
    {% if pattern %}
    PATTERN='{{ pattern }}'
    {% endif %}
    ;
    """
    table: str
    stage_name: str
    file_format: Union['NamedFileFormatTemplate', 'CustomFileFormatTemplate', str]
    fields: List[FieldMetadata] = ()
    s3_path: str = ''
    copy_options: 'CopyOptionsTemplate' = None
    audit_fields: str = None
    pattern: str = None


@dataclass
class NamedFileFormatTemplate(Templated):
    template = """
    (format_name = {{ format_name }})
    """
    format_name: str


class FileFormatType(Enum):
    CSV = 'CSV'
    JSON = 'JSON'
    AVRO = 'AVRO'
    ORC = 'ORC'
    PARQUET = 'PARQUET'
    XML = 'XML'


class FileFormatCompression(Enum):
    AUTO = 'AUTO'
    GZIP = 'GZIP'
    BZ2 = 'BZ2'
    BROTLI = 'BROTLI'
    ZSTD = 'ZSTD'
    DEFLATE = 'DEFLATE'
    RAW_DEFLATE = 'RAW_DEFLATE'
    NONE = 'NONE'


T = TypeVar('T')


class Quoted(Generic[T]):
    def __init__(self, x: T):
        self.x = x

    def __str__(self) -> str:
        return f"'{self.x}'"


@dataclass
class CustomFileFormatTemplate(Templated):
    template = """
    (
        {% for k, v in args.items() %}
        {% when k %}{{ k }} = {{ v }}{% endwhen %}
        {% endfor %}
    )
    """
    type: FileFormatType = None
    field_delimiter: Quoted[str] = None
    null_if: Tuple[str, ...] = None
    compression: FileFormatCompression = None


@dataclass
class CopyOptionsTemplate(Templated):
    template = """
    {% if force is not none %}FORCE = {{ force | str | upper }}{% endif %}
    """
    force: bool = None


if __name__ == '__main__':

    # rendered_copy = CopyTemplate(
    #     table='clients',
    #     fields= [FieldMetadata(name='src', type='variant')],
    #     stage_name='stagetestcorpdatalake',
    #     file_format=CustomFileFormatTemplate(
    #                 type=FileFormatType.JSON,
    #                 field_delimiter=Quoted(','),
    #                 null_if=('null', 'NULL'),
    #                 compression=FileFormatCompression.AUTO,
    #             ),
    #     pattern='data_client.*[.]json',
    #     audit_fields=',\nmetadata$filename,\ncurrent_timestamp(),\nmetadata$filename'
    # )

    fields = [FieldMetadata(name='src', type='variant')]
    file_format = CustomFileFormatTemplate(
        type=FileFormatType.JSON.value,
        compression=FileFormatCompression.AUTO.value,
    )
    audit_fields = ',\nmetadata$filename,\ncurrent_timestamp(),\nmetadata$filename'
    pattern = 'data_client.*[.]json',
    table = 'clients'
    stage_name = 'stagetestcorpdatalake'
    s3_path = None
    query = CopyTemplate(
        table=table,
        stage_name=stage_name,
        file_format=file_format,
        fields=fields,
        s3_path=s3_path,
        copy_options=None,
        audit_fields=audit_fields,
        pattern=None,
    ).render()


    print(query)
