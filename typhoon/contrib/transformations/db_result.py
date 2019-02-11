import csv
from io import StringIO
from typing import Sequence


def to_csv(description: Sequence, data: Sequence[Sequence]) -> str:
    csv_data = StringIO()
    writer = csv.writer(csv_data)
    writer.writerow(description)
    for row in data:
        writer.writerow(row)
    return csv_data.getvalue()
