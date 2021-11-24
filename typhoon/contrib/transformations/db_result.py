import csv
import json
from datetime import datetime, date
from io import StringIO
from json import JSONEncoder
from typing import Sequence


def to_csv(description: Sequence, data: Sequence[Sequence]) -> str:
    csv_data = StringIO(newline='')
    writer = csv.writer(csv_data, lineterminator='\n')
    writer.writerow(description)
    for row in data:
        writer.writerow(row)
    return csv_data.getvalue()


class Encoder(JSONEncoder):
    def default(self, o):
        if o.__class__.__name__ == 'Decimal':
            # print(f'Decimal encoding')
            return float(o)
        elif o.__class__.__name__ == 'UUID':
            return str(o)
        elif isinstance(o, bytes):
            # print(f'Bytes encoding len {len(o)}')
            # return base64.b64encode(o)
            return o.hex()
        elif isinstance(o, datetime):
            # print(f'Date encoding')
            return o.isoformat()
        elif isinstance(o, date):
            # print(f'Date encoding')
            return o.isoformat()
        # print(f'Default encoding')
        return JSONEncoder.default(self, o)


def to_json_records(description: Sequence, data: Sequence[Sequence]) -> str:
    result = []
    for row in data:
        record = dict(zip(description, row))
        result.append(json.dumps(record, cls=Encoder))
    return '\n'.join(result)
