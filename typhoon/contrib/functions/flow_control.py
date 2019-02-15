from time import sleep
from typing import Sequence, Iterator, Callable, Any, TypeVar


def branch(branches: Sequence, delay: int = 0) -> Iterator:
    """
    Yields each item in the sequence with an optional delay
    :param branches:
    :param delay:
    :return:
    """
    for i, b in enumerate(branches):
        if delay and i > 0:
            sleep(delay)
        yield b


T = TypeVar('T')


# noinspection PyShadowingBuiltins
def filter(data: T, filter_func: Callable[[T], bool]) -> Iterator:
    """
    Send data if the result of applying filter_func on it is True
    :param data: Any kind of data
    :param filter_func: A function that evaluates data and returns a boolean value
    :return:
    """
    if filter_func(data):
        yield data
