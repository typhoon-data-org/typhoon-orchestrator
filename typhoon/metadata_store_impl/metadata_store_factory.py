from contextlib import contextmanager
from typing import Union, Optional

from typhoon.core.config import TyphoonConfig
from typhoon.metadata_store_impl import MetadataStoreType
from typhoon.metadata_store_impl.sqlite_metadata_store import SQLiteMetadataStore


@contextmanager
def metadata_store_factory(store_type: Union[str, MetadataStoreType], config: Optional[TyphoonConfig] = None):
    if isinstance(store_type, str):
        store_type = MetadataStoreType.from_string(store_type)
    instance = None
    if store_type == MetadataStoreType.sqlite:
        instance = SQLiteMetadataStore(config)
    assert instance is not None, 'Invalid enum type'
    yield instance
    instance.close()
