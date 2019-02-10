from typing import Sequence, Iterator, Callable, Any


def branch(branches: Sequence) -> Iterator:
    """Yields each item in the sequence"""
    return iter(branches)


# noinspection PyShadowingBuiltins
def filter(data: Any, filter_func: Callable[[Any], bool]) -> Iterator:
    if filter_func(data):
        yield data
