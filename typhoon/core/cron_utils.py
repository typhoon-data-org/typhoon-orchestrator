import re
from datetime import timedelta, datetime
from typing import Union, Dict

from croniter import croniter
from dateutil.parser import parse

cron_presets: Dict[str, str] = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@quarterly': '0 0 1 */3 *',
    '@yearly': '0 0 1 1 *',
}

cron_templates = {
    'seconds': '*/{n} * * * *',
    'minutes': '*/{n} * * * *',
    'hours': '0 */{n} * * *',
    'days': '0 0 */{n} * *',
    'weeks': '0 0 * * */{n}',
    'months': '0 0 0 */{n} *',
}


def aws_schedule_to_cron(schedule: str) -> str:
    if schedule in cron_presets.keys():
        return cron_presets[schedule]
    match = re.match(r'rate\s*\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months)\s*\)', schedule)
    if match:
        n, freq = match.groups()
        freq = freq + 's' if not freq.endswith('s') else freq
        return cron_templates[freq].format(n=n)
    return schedule


def timedelta_from_cron(cron: str) -> timedelta:
    iterator = croniter(cron)
    next = iterator.get_next(datetime)
    next_next = iterator.get_next(datetime)
    next_next_next = iterator.get_next(datetime)
    delta = (next_next - next)
    if delta != (next_next_next - next_next):
        print(f'Warning: Cron expression {cron} is not regular')
    return delta


def interval_start_from_schedule_and_interval_end(schedule: str, interval_end: Union[str, datetime]) -> datetime:
    if isinstance(interval_end, str):
        interval_end = parse(interval_end)
    cron = aws_schedule_to_cron(schedule)
    iterator = croniter(cron, start_time=interval_end)
    return iterator.get_prev(datetime)


if __name__ == '__main__':
    print(aws_schedule_to_cron('rate ( 5 months )'))
    print(repr(timedelta_from_cron('* * * * *')))
    print(repr(interval_start_from_schedule_and_interval_end('rate(1 day)', '2021-02-13')))
