"""Example module for user defined functions"""
import random
from typing import Iterable

from typing_extensions import TypedDict


class Person(TypedDict):
    name: str
    surname: str
    age: int


def get_people(num: int) -> Iterable[Person]:
    """This functions yields a person for each batch"""
    for _ in range(num):
        yield {
            'name': random.choice(['John', 'Mary', 'Peter', 'Wendy', 'Mikey', 'Amanda', 'Tom', 'Anne']),
            'surname': random.choice(['Smith', 'Parker', 'Doe', 'Hendricks', 'Garcia', 'Holmes', 'Jones']),
            'age': random.randint(22, 50),
        }
