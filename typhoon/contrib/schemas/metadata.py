from dataclasses import dataclass


@dataclass
class FieldMetadata:
    name: str
    type: str
    precision: int = None
    scale: int = None
    pk: bool = False
