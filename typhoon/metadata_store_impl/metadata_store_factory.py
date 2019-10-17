from typing import Union

from typhoon.metadata_store_impl import MetadataStoreType
from typhoon.metadata_store_impl.dynamodb_metadata_store import DynamodbMetadataStore
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore


def metadata_store_factory(store_type: Union[str, MetadataStoreType]):
    if isinstance(store_type, str):
        store_type = MetadataStoreType.from_string(store_type)
    if store_type == MetadataStoreType.sqlite:
        return SQLiteMetadataStore
    elif store_type == MetadataStoreType.dynamodb:
        return DynamodbMetadataStore
    assert False, 'Invalid enum type'
