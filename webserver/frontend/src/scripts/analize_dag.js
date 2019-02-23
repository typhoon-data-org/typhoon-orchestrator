import {
  AnalysisException, check_eol,
  check_not_eof,
  check_not_eol,
  get_tokens_block,
  push_error_msg,
  push_warning_msg
} from "./ace_helper";
import {
  A_CRON_DAY_OF_MONTH,
  A_CRON_DAY_OF_WEEK,
  A_CRON_HOURS,
  A_CRON_MINUTES,
  A_CRON_MONTH,
  A_CRON_YEAR
} from "./cron_checker";


export function A_DAG() {
  try {
    let line = A_NAME();
    line = A_SCHEDULE_INTERVAL(line);
    line = A_ACTIVE(line);
    // A_NODES();
    // A_EDGES();
  } catch (e) {
    if (!(e instanceof AnalysisException)) {
      throw e;
    }
  }
}

function A_NAME() {
  let tokens, line;
  [tokens, line] = get_tokens_block();

  let tk = tokens.shift();
  if ((tk.type !== "meta.tag") || (tk.value !== 'name')) {
    push_error_msg("Expected 'name:' at the top of DAG definition", tk.line);
    throw new AnalysisException();
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected DAG name, not end of line");
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':' after name");
    throw new AnalysisException();
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected DAG name, not end of line");
  if (tk.type !== 'text') {
    push_error_msg('Dag name should be text');
    throw new AnalysisException();
  }
  check_dag_name(tk);

  tk = tokens.shift();
  check_eol(tk, 'Expected line break');

  tk = tokens.shift();
  if (tk !== undefined) {
    check_not_eof(tk, 'Expected schedule interval definition');
  }

  return line;
}

function check_dag_name(tk) {
  if (!tk.value.startsWith(' ')) {
    push_warning_msg("Add a space after 'name:' tag");
  }
  if (tk.value === ' ') {
    push_error_msg('No DAG name specified');
    throw new AnalysisException();
  }
  let dag_name = tk.value.trim();
  let valid_name = /^[a-zA-Z][\w]*$/.test(dag_name);
  if (!valid_name) {
    let invalid_characters = dag_name.replace(/^[a-zA-Z][\w]*$/g, '');
    push_error_msg('Invalid DAG name: Must be composed of letters, numbers and underscores', tk.line);
    throw new AnalysisException();
  }
}

function A_SCHEDULE_INTERVAL(start_line) {
  let tokens, end_line;
  [tokens, end_line] = get_tokens_block(start_line);

  let tk = tokens.shift();
  if ((tk.type !== "meta.tag") || (tk.value !== 'schedule-interval')) {
    push_error_msg("Expected 'schedule-interval:'", tk.line);
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected schedule interval, not end of line");
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':' after name");
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected schedule interval, not end of line");
  if ((tk.type !== 'text') && (tk.type !== 'string') && (tk.type !== 'constant.numeric')) {
    push_error_msg('Schedule interval should be text', tk.line);
  }
  check_cron_expression(tk);

  tk = tokens.shift();
  check_eol(tk, 'Expected line break');

  tk = tokens.shift();
  if (tk !== undefined) {
    check_not_eof(tk, 'Expected nodes definition');
  }

  return end_line;
}

function check_cron_expression(tk) {
  if (!tk.value.startsWith(' ')) {
    push_warning_msg("Add a space after 'schedule-interval:' tag");
  }
  if (tk.value === ' ') {
    push_error_msg('No schedule interval specified');
    throw new AnalysisException();
  }
  let schedule_interval = tk.value.trim().replace(/['"/]/g, "");

  if (schedule_interval.startsWith('rate(')) {
    let rate_re = /^rate\(\s*([^)]+)/g;
    let match = rate_re.exec(schedule_interval);
    if (match == null) {
      push_error_msg('Invalid rate expression', tk.line);
      throw new AnalysisException();
    }
    let num, interval;
    let rate_exp = match[1].split(' ');
    if (rate_exp.length !== 2) {
      push_error_msg('Rate expression should be made of two parts. Found ' + rate_exp.length, tk.line);
      throw new AnalysisException();
    }
    [num, interval] = rate_exp;
    let valid_intervals = ['minute', 'minutes', 'hour', 'hours', 'day', 'days'];
    if (!valid_intervals.includes(interval)) {
      push_error_msg('Interval should be in ' + JSON.stringify(valid_intervals) + '. Found: ' + interval, tk.line);
      throw new AnalysisException();
    }
    if ((num === '1') && interval.endsWith('s')) {
      push_error_msg('Change ' + interval + ' to ' + interval.slice(0, -1), tk.line);
      throw new AnalysisException();
    } else if ((parseInt(num) > 1) && !interval.endsWith('s')) {
      push_error_msg('Change ' + interval + ' to ' + interval + 's', tk.line);
      throw new AnalysisException();
    }
  } else {
    let intervals = schedule_interval.split(' ');

    let minutes = intervals[0];
    A_CRON_MINUTES(minutes, tk.line);

    if (intervals.length < 2) {
      push_error_msg('Cron expression is missing hours', tk.line);
      throw new AnalysisException();
    }
    let hours = intervals[1];
    A_CRON_HOURS(hours, tk.line);

    if (intervals.length < 3) {
      push_error_msg('Cron expression is missing day of month', tk.line);
      throw new AnalysisException();
    }
    let day_of_month = intervals[2];
    A_CRON_DAY_OF_MONTH(day_of_month, tk.line);

    if (intervals.length < 4) {
      push_error_msg('Cron expression is missing month', tk.line);
      throw new AnalysisException();
    }
    let month = intervals[3];
    A_CRON_MONTH(month, tk.line);

    if (intervals.length < 5) {
      push_error_msg('Cron expression is missing day of week', tk.line);
      throw new AnalysisException();
    }
    let day_of_week = intervals[4];
    A_CRON_DAY_OF_WEEK(day_of_week, tk.line);

    if (intervals.length < 6) {
      push_error_msg('Cron expression is missing year', tk.line);
      throw new AnalysisException();
    }
    let year = intervals[5];
    A_CRON_YEAR(year, tk.line);
  }
}

function A_ACTIVE(start_line) {
  let tokens, end_line;
  [tokens, end_line] = get_tokens_block(start_line);

  let tk = tokens.shift();
  if ((tk.type !== "meta.tag") || (tk.value !== 'active')) {
    // Active is optional so skip if not defined
    return start_line;
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected active definition, not end of line");
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':' after name", tk.line);
    throw new AnalysisException();
  }

  tk = tokens.shift();
  check_not_eol(tk, "Expected active definition, not end of line");
  if (tk.type !== 'constant.language.boolean') {
    push_error_msg('Active definition should be boolean', tk.line);
    throw new AnalysisException();
  }

  tk = tokens.shift();
  check_eol(tk, 'Expected line break');

  tk = tokens.shift();
  if (tk !== undefined) {
    check_not_eof(tk, 'Expected nodes definition');
  }

  return end_line;
}
