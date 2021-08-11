from time import sleep
from typing import Sequence, Iterator, Callable, TypeVar, Iterable

from typhoon.core import SKIP_BATCH

T = TypeVar('T')


def branch(branches: Sequence[T], delay: int = 0) -> Iterable[T]:
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


# noinspection PyShadowingBuiltins
def filter(data: T, filter_func: Callable[[T], bool]) -> Iterator:
    """
    Send data if the result of applying filter_func on it is True
    :param data: Any kind of data
    :param filter_func: A function that evaluates data and returns a boolean value
    :return:
    """
    yield data if filter_func(data) else SKIP_BATCH


def do(func: Callable, args: list = None, kwargs: dict = None):
    yield func(*(args or []), **(kwargs or {}))
