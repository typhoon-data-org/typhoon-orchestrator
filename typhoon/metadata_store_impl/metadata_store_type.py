from enum import Enum


class MetadataStoreType(Enum):
    sqlite = 'sqlite'

    @staticmethod
    def from_string(s: str) -> 'MetadataStoreType':
        return MetadataStoreType[s]
