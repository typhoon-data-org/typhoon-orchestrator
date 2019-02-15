from time import sleep
from typing import Sequence, Iterator, Callable, Any


def branch(branches: Sequence, delay: int = 0) -> Iterator:
    """Yields each item in the sequence"""
    for b in branches:
        if delay:
            sleep(delay)
        yield b


# noinspection PyShadowingBuiltins
def filter(data: Any, filter_func: Callable[[Any], bool]) -> Iterator:
    if filter_func(data):
        yield data
