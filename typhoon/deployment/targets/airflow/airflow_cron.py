import re
from datetime import timedelta, datetime

from croniter import croniter

cron_templates = {
    'minutes': '*/{n} * * * *',
    'hours': '0 */{n} * * *',
    'days': '0 0 */{n} * *',
    'months': '0 0 0 */{n} *',
}


def aws_schedule_to_airflow_cron(schedule: str) -> str:
    match = re.match(r'rate\s*\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days|month|months)\s*\)', schedule)
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


if __name__ == '__main__':
    print(aws_schedule_to_airflow_cron('rate ( 5 months )'))
    print(repr(timedelta_from_cron('* * * * *')))
