import {AnalysisException, push_error_msg} from "./ace_helper";

export function A_CRON_MINUTES(minutes, line) {
  if (minutes === '*')
    return;

  let parts = minutes.split(',');
  for (let i = 0; i < parts.length; i++) {
    check_cron_range(parts[i], line, 'minute', 0, 59);
  }
}

function check_cron_range(part, line, interval, min, max) {
  let units = [];
  if (part.includes('/') && part.includes('-')) {
    push_error_msg("Invalid cron: " + interval + "s can't contain both / and -", line);
    throw new AnalysisException();
  }
  if (part.includes('/')) {
    units = part.split('/');
  } else if (part.includes('-') && isNaN(part)) {
    units = part.split('-');
  } else {
    units = [part];
  }

  units.forEach(unit => {
    let minute = parseInt(unit);
    if (isNaN(minute)) {
      push_error_msg('Invalid cron: ' + interval + ' should be an integer', line);
      throw new AnalysisException();
    }
    if (minute < min) {
      push_error_msg('Invalid cron: ' + interval + ' should be > ' + min + '. Found: ' + minute, line);
      throw new AnalysisException();
    }
    if (minute > max) {
      push_error_msg('Invalid cron: ' + interval + ' should be < ' + max + '. Found ' + minute, line);
      throw new AnalysisException();
    }
  })
}

export function A_CRON_HOURS(hours, line) {
  if (hours === '*')
    return;

  let parts = hours.split(',');
  for (let i = 0; i < parts.length; i++) {
    check_cron_range(parts[i], line, 'hour', 0, 23);
  }
}

export function A_CRON_DAY_OF_MONTH(day_of_month, line) {
  if (day_of_month === '*')
    return;

  let parts = day_of_month.split(',');
  for (let i = 0; i < parts.length; i++) {
    check_cron_range(parts[i], line, 'day of month', 1, 31);
  }
}

export function A_CRON_MONTH(month, line) {
  if (month === '*')
    return;

  let parts = month.split(',');
  for (let i = 0; i < parts.length; i++) {
    check_cron_range(parts[i], line, 'month', 1, 12);
  }
}

export function A_CRON_DAY_OF_WEEK(day_of_week, line) {
  if (day_of_week === '*')
    return;

  let parts = day_of_week.split(',');
  for (let i = 0; i < parts.length; i++) {
    check_cron_range(parts[i], line, 'month', 1, 7);
  }
}

export function A_CRON_YEAR(year, line) {
  if (year === '*')
    return;

  let parts = year.split(',');
  for (let i = 0; i < parts.length; i++) {
    check_cron_range(parts[i], line, 'month', 1, 7);
  }
}
