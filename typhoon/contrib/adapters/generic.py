from typing import TypeVar, Dict

T = TypeVar('T')


def identity(data: T, key: str) -> Dict[str, T]:
    return {key: data}
