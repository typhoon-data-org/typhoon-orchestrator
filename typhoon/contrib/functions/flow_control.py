from typing import Sequence, Iterator


def branch(branches: Sequence) -> Iterator:
    """Yields each item in the sequence"""
    return iter(branches)
