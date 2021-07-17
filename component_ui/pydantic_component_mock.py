from pydantic import BaseModel
from typing import Literal, Union

"""
name: glob_compress_and_copy

args:
  source_hook: FileSyestemHookInterface
  destination_hook: FileSyestemHookInterface
  pattern: str
  destination_path: jinja2.Template
  compression: gzip or zlib
"""


class FileHookType(BaseModel):
    hook_class: Literal['LocalStorageHook', 'S3Hook']


class DBHookType(BaseModel):
    hook_class: Literal['SqliteHook', 'DbApiHook', 'Snowflake']


class Singer_Demultiplexer_Model(BaseModel):
    """
    Component argument description
    """
    message: str

class Glob_Compress_Model(BaseModel):
    """
    Component argument description
    """
    source_hook_2: FileHookType
    pattern: str
    destination_path: str
    compression = ["gzip", "zlib"]

class CSV_To_Snowflake_Model(BaseModel):
    """
    Component argument description
    """
    source_hook: FileHookType
    source_path: str
    source_glob_pattern: str
    s3_hook: FileHookType
    s3_compression = str
    snowflake_hook: DBHookType
    snowflake_stage_name: str

class Component(BaseModel):
    component_name: Union[Glob_Compress_Model, CSV_To_Snowflake_Model, Singer_Demultiplexer_Model]


# this is equivalent to json.dumps(MainModel.schema(), indent=2):
c = CSV_To_Snowflake_Model(
    source_hook={'hook_class':'LocalStorageHook'},
    source_path="/",
    source_glob_pattern='*.csv',
    s3_hook={'hook_class':'S3Hook'},
    s3_compression='gzip',
    snowflake_hook={'hook_class':'Snowflake'},
    snowflake_stage_name='corp_data_stage'
)

# print(Singer_Demultiplexer_Model.schema_json(indent=2))
# print(Glob_Compress_Model.schema_json(indent=2))
# print(CSV_To_Snowflake_Model.schema_json(indent=2))
#print(Component.schema_json(indent=2))
#
# l = Component.schema()['properties']['component_name']['anyOf']
#list_c = [x['$ref'].split('/')[2] for x in l]
#print(list_c)
#print(c.json(indent=2))
#print(c.json())
