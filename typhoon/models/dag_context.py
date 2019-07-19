import json
from datetime import datetime
from typing import NamedTuple, Dict

import dateutil.parser


class DagContext(NamedTuple):
    execution_date: datetime
    ds: str
    ds_nodash: str
    ts: str
    etl_timestamp: str

    @staticmethod
    def from_date_string(date_string) -> 'DagContext':
        execution_date = dateutil.parser.parse(date_string)
        return DagContext(
            execution_date=execution_date,
            ds=execution_date.strftime('%Y-%m-%d'),
            ds_nodash=execution_date.strftime('%Y-%m-%d').replace('-', ''),
            ts=execution_date.strftime('%Y-%m-%dT%H:%M:%S'),
            etl_timestamp=datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        )

    def to_dict(self) -> Dict:
        d = self._asdict()
        d['execution_date'] = str(d['execution_date'])
        return d

    @staticmethod
    def from_dict(d: Dict) -> 'DagContext':
        return DagContext.from_date_string(d['execution_date'])
