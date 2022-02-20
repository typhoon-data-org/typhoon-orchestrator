import re
from datetime import timedelta, datetime
from typing import Optional, Union, Dict

from croniter import croniter
from dateutil.parser import parse


CRON_REGEX = (
    r'^((?:[1-5]?\d|\*|,|-|/)(?:/\d+)?)\s+' +
    r'((?:1\d|2[0-3]|\d|\*|,\-\/)(?:/\d+)?)\s+' +
    r'((?:[1-2]\d|3[01]|\d|\*|,\-\/)(?:/\d+)?)' +
    r'\s+((?:1[0-2]|[1-9]|(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)|\*|,\-\/)(?:/\d+)?)\s+' +
    r'((?:[0-6]|(?:MON|TUE|WED|THU|FRI|SAT|SUN)|\*|,|-|/)(?:/\d+)?)$'
)

RATE_REGEX = r'rate\s*\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months)\s*\)'

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
    match = re.match(RATE_REGEX, schedule)
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


def dag_schedule_to_aws_cron(schedule: str) -> Optional[str]:
    if schedule in cron_presets.keys():
        return dag_schedule_to_aws_cron(cron_presets[schedule])
    
    match = re.match(RATE_REGEX, schedule)
    if match:
        n, freq = match.groups()
        freq = freq + 's' if not freq.endswith('s') else freq
        return dag_schedule_to_aws_cron(cron_templates[freq].format(n=n))
    
    matches = re.match(CRON_REGEX, schedule)
    if not matches:
        return None
    minute, hour, day_of_month, month, day_of_week = matches.groups()
    try:
        day_of_week = int(day_of_week) + 1
    except ValueError:
        pass
    if (
        day_of_month == '*' and day_of_week == '*' or
        day_of_month != '*' and day_of_week == '*'
    ):
        day_of_week = '?'
    elif day_of_month == '*' and day_of_week != '*':
        day_of_month = '?'
    return f'cron({minute} {hour} {day_of_month} {month} {day_of_week} *)'

if __name__ == '__main__':
    print(aws_schedule_to_cron('rate ( 5 months )'))
    print(repr(timedelta_from_cron('* * * * *')))
    print(repr(interval_start_from_schedule_and_interval_end('rate(1 day)', '2021-02-13')))
