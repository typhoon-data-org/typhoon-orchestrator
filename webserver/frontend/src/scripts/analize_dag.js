import {
  AnalysisException,
  check_dedent, check_eof,
  check_eol,
  check_indent,
  check_meta_tag,
  check_not_eof,
  check_not_eol,
  check_semicolon,
  check_type,
  check_type_value,
  get_indents,
  get_tokens_block, is_dedent, is_eof,
  is_eol, is_indent,
  is_special_var, is_type,
  is_type_value,
  is_valid_special_var,
  num_lines,
  push_error_msg,
  push_warning_msg,
  skip_blank_lines, skip_to_next_block,
  stringify_until_eol
} from "./ace_helper";
import {
  A_CRON_DAY_OF_MONTH,
  A_CRON_DAY_OF_WEEK,
  A_CRON_HOURS,
  A_CRON_MINUTES,
  A_CRON_MONTH,
  A_CRON_YEAR
} from "./cron_checker";

export let NODE_NAMES=[];
export let EDGE_CONFIGS={};


export function A_DAG() {
  EDGE_CONFIGS = {};
  NODE_NAMES = [];
  try {
    let line = A_NAME();
    line = A_SCHEDULE_INTERVAL(line);
    line = A_ACTIVE(line);
    line = A_NODES(line);
    line = skip_to_next_block(line);
    A_EDGES(line);
    return true;
  } catch (e) {
    if (!(e instanceof AnalysisException)) {
      throw e;
    }
    return false;
  }
}

function A_NAME() {
  let tokens, line;
  [tokens, line] = get_tokens_block();

  let tk = tokens.shift();
  check_meta_tag(tk, "Expected 'name:' at the top of DAG definition", 'name');
  tk = tokens.shift();
  check_not_eol(tk, "Expected DAG name, not end of line");
  check_semicolon(tk);
  tk = tokens.shift();
  check_not_eol(tk, "Expected DAG name, not end of line");
  check_type(tk, 'text', 'DAG name should be text');
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
  check_meta_tag(tk, "Expected 'schedule_interval:'", 'schedule_interval');
  // if ((tk.type !== "meta.tag") || (tk.value !== 'schedule-interval')) {
  //   push_error_msg("Expected 'schedule-interval:'", tk.line);
  // }
  tk = tokens.shift();
  check_not_eol(tk, "Expected schedule interval, not end of line");
  check_semicolon(tk);
  tk = tokens.shift();
  check_not_eol(tk, "Expected schedule interval, not end of line");
  check_type(tk, ['text', 'string', 'constant.numeric', 'Schedule interval should be text']);
  // if ((tk.type !== 'text') && (tk.type !== 'string') && (tk.type !== 'constant.numeric')) {
  //   push_error_msg('Schedule interval should be text', tk.line);
  // }
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
  if (!tk.value.startsWith(' ') && !tk.value.startsWith('"')) {
    push_warning_msg("Add a space after 'schedule-interval:' tag", tk.line);
  }
  if (tk.value === ' ') {
    push_error_msg('No schedule interval specified', tk.line);
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

function A_NODES(start_line) {
  let tokens, end_line;
  [tokens, end_line] = get_tokens_block(start_line);

  let tk = tokens.shift();
  if ((tk.type !== "meta.tag") || (tk.value !== 'nodes')) {
    push_error_msg("Expected 'nodes:' definition", tk.line);
    throw new AnalysisException();
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected ':'");
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':'", tk.line);
    throw new AnalysisException();
  }

  tk = tokens.shift();
  check_eol(tk, "Expected line break");

  tk = tokens.shift();
  if (tk === undefined) {
    push_error_msg("Expected indent", num_lines() - 1);
    throw new AnalysisException();
  }
  check_indent(tk, "Expected indent");

  end_line = A_NODE(tokens);
  if (tokens[0] === undefined) {
    return end_line;
  }
  if (is_indent(tokens[0])) {
    tokens.shift();
  }
  check_not_eof(tokens[0], "Expected node definition or 'edges:' tag", tk.line);
  let indents = get_indents(tokens[0].line);
  while (tokens[0].type === 'meta.tag' && indents === 1) {
    end_line = A_NODE(tokens);
    if (tokens[0] === undefined) {
      return end_line;
    }
    if (is_indent(tokens[0])) {
      tokens.shift();
    }
    check_not_eof(tokens[0], "Expected node definition or 'edges:' tag", tk.line);
    indents = get_indents(tokens[0].line);
  }

  // if (indents > 1) {
  //   push_error_msg("Wrong indentation. Expected 1 indent for node definition" +
  //     " or 0 for 'edges:' tag. Found " + indents, tokens[0].line);
  //   throw new AnalysisException();
  // }

  return end_line;
}

function A_NODE(tokens) {
  let tk = tokens.shift();
  check_not_eol(tk, "Expected node definition, not end of line");
  check_meta_tag(tk, 'Expected node definition');
  let node_name = tk.value;
  NODE_NAMES.push(node_name.trim());
  tk = tokens.shift();
  check_not_eol(tk, "Expected ':'");
  check_type_value(tk, 'keyword', ':', "Expected ':'");
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_indent(tk, "Expected indent");
  tk = tokens.shift();
  check_meta_tag(tk, "Expected 'function:' definition", 'function');
  tk = tokens.shift();
  check_semicolon(tk);
  tk = tokens.shift();
  check_type(tk, 'text', 'Invalid function');
  A_FUNCTION(tk);
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_meta_tag(tk, "Expected 'config:' definition", 'config');
  tk = tokens.shift();
  check_semicolon(tk);
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_indent(tk, "Expected indent");
  let end_line = A_CONFIG(tokens, node_name, false);
  return end_line;
}

function A_FUNCTION(tk) {
  let parts = tk.value.trim().split('.');
  if (parts[0] !== 'typhoon' && parts[0] !== 'functions') {
    push_error_msg('Function should be built-in (typhoon.) or used refined (functions.)', tk.line);
    throw new AnalysisException();
  }
  if (parts.length < 3 || (parts.length === 3 && parts[2] === '')) {
    push_error_msg('Incomplete function definition', tk.line);
    throw new AnalysisException();
  }

  let function_name = tk.value.trim();
  let valid_name = /^[a-zA-Z][\w.]*$/.test(function_name);
  if (!valid_name) {
    push_error_msg('Invalid function name: Must be composed of letters, numbers, dots and underscores', tk.line);
    throw new AnalysisException();
  }
}

function A_CONFIG(tokens, parent_name, is_edge) {
  let tk = tokens.shift();
  check_meta_tag(tk, "Expected meta tag");
  let config_name = tk.value.trim();
  let valid_name = /^[a-zA-Z][\w_]*(\s?=>\s?APPLY)?$/.test(config_name);
  if (!valid_name) {
    push_error_msg("Invalid name. Use letters, numbers and underscores. May end with ' => APPLY'", tk.line);
    throw new AnalysisException();
  }

  tk = tokens.shift();
  check_semicolon(tk);

  let end_line, value;
  if (/^[a-zA-Z][\w_]*(\s?=>\s?APPLY)$/.test(config_name)) {
    [end_line, value] = A_APPLY(tokens);
    if (is_edge) {
      EDGE_CONFIGS[parent_name][config_name.split(' => APPLY')[0]] = {
        'apply': true,
        'contents': value,
      };
    }
  } else {
    [end_line, value] = A_VALUE(tokens);
    if (is_edge) {
      EDGE_CONFIGS[parent_name][config_name.split(' => APPLY')[0]] = {
        'apply': false,
        'contents': value,
      }
    }
  }

  if (tokens[0] !== undefined && tokens[0].type === 'meta.tag') {
    return A_CONFIG(tokens, parent_name, is_edge);
  }
  if (tokens[0] !== undefined && is_dedent(tokens[0])) {
    tokens.shift();
    end_line++;
  }
  return end_line;
}

function A_APPLY(tokens) {
  let transformations = [];
  let transformation;
  if (!is_eol(tokens[0])) {
    transformation = A_APPLY_LINE(tokens, false);
    transformations.push(transformation);
    return [tokens[0].line, transformations]
  } else {
    let tk = tokens.shift();  // Skip end of line
    tk = tokens.shift();  // Skip end of line
    check_indent(tk, 'Expected indent');
    tk = tokens.shift();
    if (tk.type !== 'list.markup' || tk.value.trim() !== '-') {
      push_error_msg("Expected list ('-')", tk.line);
    }
    transformation = A_APPLY_LINE(tokens, true);
    transformations.push(transformation);

    tk = tokens.shift();
    while(tk.type === 'list.markup' && tk.value.trim() === '-') {
      transformation = A_APPLY_LINE(tokens, true);
      transformations.push(transformation);
      tk = tokens.shift();
    }
    check_dedent(tk, 'Expected dedent');
    return [tk.line, transformations];
  }
}

function A_APPLY_LINE(tokens, special_var_nums) {
  stringify_until_eol(tokens);
  let tk = tokens.shift();
  let apply_line_str = tk.value;
  let special_vars = tk.value.match(/\$[\w]+/g);
  if (special_vars != null) {
    special_vars.forEach(special_var => {
      if (!is_valid_special_var(special_var, special_var_nums)) {
        push_error_msg(
          'Invalid special variable ' + special_var + ". Must be '$SOURCE', '$DAG_CONFIG' or $BATCH_NUM",
          tk.line
        );
        throw new AnalysisException();
      }
    });
  }
  // We know the next token is end of line
  tokens.shift();
  return apply_line_str;
}

function A_VALUE(tokens) {
  let value;
  let tk = tokens.shift();
  if (is_eol(tk)) {
    tk = tokens.shift();
    check_indent(tk, 'Expected indent');
    if (is_type_value(tokens[0], 'list.markup', '-')) {
      value = A_YAML_LIST(tokens);
      if (is_dedent(tokens[0])) {
        tk = tokens.shift();
      }
    }
    else if (is_type(tokens[0], 'meta.tag')) {
      value = A_YAML_DICT(tokens);
      if (is_dedent(tokens[0])) {
        tk = tokens.shift();
      }
    }
    return [tk.line, value];
  }
  else if (is_type_value(tk, 'paren.lparen', /[[{][[{]+/)) {
    // Workaround because ace groups parenthesis together. Separate each into its own token
    let parens = tk.value.replace(/\s+/g, '').split('');
    parens.reverse().forEach(paren => {
      tokens.unshift({
        type: 'paren.lparen',
        value: paren,
        line: tk.line,
      })
    });
    tk = tokens.shift();
  }
  if (is_type_value(tk, 'paren.lparen', '[')) {
    value = A_ARRAY(tokens);
  } else if (is_type_value(tk, 'paren.lparen', '{')){
    value = A_DICT(tokens);
  } else {
    check_type(tk, ['text', 'constant.numeric', 'constant.language.boolean', 'string'], 'Unrecognized type');
    if (is_type(tk, 'text') || is_type(tk, 'string')) {
      value = '"' + tk.value + '"'
    } else {
      value = tk.value;
    }
  }
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  return [tk.line, value];
}

function A_YAML_LIST(tokens) {
  let elements = [];
  let tk = tokens.shift();
  while (is_type_value(tk, 'list.markup', '-')) {
    if (is_eol(tokens[0])) {
      tk = tokens.shift();
      tk = tokens.shift();
      check_indent(tk, 'Expected indent');
      if (is_type(tokens[0], 'meta.tag')) {
        let e = A_YAML_DICT(tokens);
        elements.push(e);
        tk = tokens.shift();
        check_dedent(tk, 'Expected dedent');
        tk = tokens.shift();
      } else {
        check_type(tokens[0], 'list.markup', 'Expected nested list');
        let e = A_YAML_LIST(tokens);
        elements.push(e);
        tk = tokens.shift();
        check_dedent(tk, 'Expected dedent');
        tk = tokens.shift();
      }
    } else {
      let end_line, val;
      [end_line, val] = A_VALUE(tokens);
      elements.push(val);
      tk = tokens.shift();
    }
  }
  tokens.unshift(tk);
  return '[' + elements.join(', ') + ']';
}

function A_YAML_DICT(tokens) {
  let key_vals = {};
  let tk = tokens.shift();
  while (is_type(tk, 'meta.tag')) {
    let k = tk.value.trim();
    tk = tokens.shift();
    check_semicolon(tk);
    if (is_eol(tokens[0])) {
      tk = tokens.shift();
      tk = tokens.shift();
      check_indent(tk, 'Expected indent');
      if (is_type(tokens[0], 'meta.tag')) {
        let v = A_YAML_DICT(tokens);
        key_vals[k] = v;
        tk = tokens.shift();
        check_dedent(tk, 'Expected dedent');
        tk = tokens.shift();
      } else {
        check_type(tokens[0], 'list.markup', 'Expected nested list');
        let v = A_YAML_LIST(tokens);
        key_vals[k] = v;
        tk = tokens.shift();
        check_dedent(tk, 'Expected dedent');
        tk = tokens.shift();
      }
    } else {
      let end_line, val;
      [end_line, val] = A_VALUE(tokens);
      key_vals[k] = val;
      tk = tokens.shift();
    }
  }
  tokens.unshift(tk);
  return JSON.stringify(key_vals);
}

function A_ARRAY(tokens) {
  let elements;
  elements = [];
  let tk = tokens.shift();
  if (is_type_value(tk, 'paren.rparen', /][\]}]+/)) {
    // Workaround because ace groups parenthesis together. Separate each into its own token
    let parens = tk.value.replace(/\s+/g, '').split('');
    parens.reverse().forEach(paren => {
      tokens.unshift({
        type: 'paren.rparen',
        value: paren,
        line: tk.line,
      })
    });
    tk = tokens.shift();
  }
  while (!is_type_value(tk, 'paren.rparen', ']')) {
    check_not_eol(tk, "Missing closing bracket ']'");
    if (is_type_value(tk, 'paren.lparen', /[[{][[{]+/)) {
      // Workaround because ace groups parenthesis together. Separate each into its own token
      let parens = tk.value.replace(/\s+/g, '').split('');
      parens.reverse().forEach(paren => {
        tokens.unshift({
          type: 'paren.lparen',
          value: paren,
          line: tk.line,
        })
      });
      tk = tokens.shift();
    }
    if (is_type_value(tk, 'paren.lparen', '[')) {
      let array = A_ARRAY(tokens);
      elements.push(array);
    } else if (is_type_value(tk, 'paren.lparen', /\s*{\s*/)) {
      let dict = A_DICT(tokens);
      elements.push(dict);
    } else {
      check_type(
        tk, ['constant.numeric', 'constant.language.boolean', 'string'], 'Unrecognized type');
      elements.push(tk.value);
    }
    tk = tokens.shift();
    check_not_eol(tk, "Missing closing bracket ']'");
    if (is_type_value(tk, 'paren.rparen', /][\]}]+/)) {
      // Workaround because ace groups parenthesis together. Separate each into its own token
      let parens = tk.value.replace(/\s+/g, '').split('');
      parens.reverse().forEach(paren => {
        tokens.unshift({
          type: 'paren.rparen',
          value: paren,
          line: tk.line,
        })
      });
      tk = tokens.shift();
    }
    if (is_type_value(tk, 'text', ',') || is_type_value(tk, 'text', ', ')) {
      tk = tokens.shift();
    } else if (!is_type_value(tk, 'paren.rparen', ']')) {
      push_error_msg("Expected ',' or ']'", tk.line);
      throw new AnalysisException();
    }
  }
  return '[' + elements.join(', ') + ']';
}

function A_DICT(tokens) {
  let key_vals = {};
  let tk = tokens.shift();
  if (is_type_value(tk, 'paren.rparen', /}[\]}]+/)) {
    // Workaround because ace groups parenthesis together. Separate each into its own token
    let parens = tk.value.replace(/\s+/g, '').split('');
    parens.reverse().forEach(paren => {
      tokens.unshift({
        type: 'paren.rparen',
        value: paren,
        line: tk.line,
      })
    });
    tk = tokens.shift();
  }
  while (!is_type_value(tk, 'paren.rparen', '}')) {
    check_not_eol(tk, "Missing closing curly bracket '}'");
    check_meta_tag(tk, 'Expected meta tag');
    let k = tk.value;
    tk = tokens.shift();
    check_not_eol(tk, "Missing ':'");
    check_semicolon(tk);

    tk = tokens.shift();
    if (is_type_value(tk, 'paren.lparen', /[[{][[{]+/)) {
      // Workaround because ace groups parenthesis together. Separate each into its own token
      let parens = tk.value.replace(/\s+/g, '').split('');
      parens.reverse().forEach(paren => {
        tokens.unshift({
          type: 'paren.lparen',
          value: paren,
          line: tk.line,
        })
      });
      tk = tokens.shift();
    }
    if (is_type_value(tk, 'paren.lparen', /\s*\[\s*/)) {
      let array = A_ARRAY(tokens);
      key_vals[k] = array;
    } else if (is_type_value(tk, 'paren.lparen', /\s*{\s*/)) {
      let dict = A_DICT(tokens);
      key_vals[k] = dict;
    } else {
      check_type(
        tk, ['constant.numeric', 'constant.language.boolean', 'string'], 'Unrecognized type');
      key_vals[k] = tk.value;
    }

    tk = tokens.shift();
    check_not_eol(tk, "Missing closing bracket '}'");
    if (is_type_value(tk, 'paren.rparen', /}[\]}]+/)) {
      // Workaround because ace groups parenthesis together. Separate each into its own token
      let parens = tk.value.replace(/\s+/g, '').split('');
      parens.reverse().forEach(paren => {
        tokens.unshift({
          type: 'paren.rparen',
          value: paren,
          line: tk.line,
        })
      });
      tk = tokens.shift();
    }
    if (is_type_value(tk, 'text', ',') || is_type_value(tk, 'text', ', ')) {
      tk = tokens.shift();
    } else if (!is_type_value(tk, 'paren.rparen', '}')) {
      push_error_msg("Expected ',' or '}'", tk.line);
      throw new AnalysisException();
    }
  }
  return JSON.stringify(key_vals);
}

function A_EDGES(start_line) {
  let tokens, end_line;
  [tokens, end_line] = get_tokens_block(start_line);

  let tk = tokens.shift();
  if ((tk.type !== "meta.tag") || (tk.value !== 'edges')) {
    push_error_msg("Expected 'edges:' definition", tk.line);
    throw new AnalysisException();
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected ':'");
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':'", tk.line);
    throw new AnalysisException();
  }

  tk = tokens.shift();
  check_eol(tk, "Expected line break");

  tk = tokens.shift();
  if (tk === undefined) {
    push_error_msg("Expected indent", num_lines() - 1);
    throw new AnalysisException();
  }
  check_indent(tk, "Expected indent");

  A_EDGE(tokens);
  while (is_eol(tokens[0]) || is_indent(tokens[0])) {
    tokens.shift();
  }
  if (is_eof(tokens[0])) {
    return;
  }
  let indents = get_indents(tokens[0].line);
  while (tokens[0].type === 'meta.tag' && indents === 1) {
    A_EDGE(tokens);
    if (is_eof(tokens[0])) {
      return;
    }
    indents = get_indents(tokens[0].line);
  }

  // if (indents > 1) {
  //   push_error_msg("Wrong indentation. Expected 1 indent for edge definition" +
  //     " or end of file. Found " + indents, tokens[0].line);
  //   throw new AnalysisException();
  // }

  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_eof(tk, 'Expected file end');
}

function A_EDGE(tokens) {
  let tk = tokens.shift();
  check_not_eol(tk, "Expected edge definition, not end of line");
  check_meta_tag(tk, 'Expected edge definition');
  let edge_name = tk.value;
  EDGE_CONFIGS[edge_name] = {};
  tk = tokens.shift();
  check_not_eol(tk, "Expected ':'");
  check_type_value(tk, 'keyword', ':', "Expected ':'");
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_indent(tk, "Expected indent");
  tk = tokens.shift();
  if (is_type_value(tk, 'meta.tag', 'async')) {
    tk = tokens.shift();
    check_semicolon(tk);
    tk = tokens.shift();
    check_type(tk, 'constant.language.boolean', 'Expected boolean value');
    tk = tokens.shift();
    check_eol(tk, 'Expected line break');
    tk = tokens.shift();
  }
  check_meta_tag(tk, "Expected 'source:' definition", 'source');
  tk = tokens.shift();
  check_semicolon(tk);
  tk = tokens.shift();
  check_type(tk, 'text', 'Invalid source');
  A_ID(tk, 'Invalid source identifier');
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_meta_tag(tk, "Expected 'adapter:' definition", 'adapter');
  tk = tokens.shift();
  check_semicolon(tk);
  tk = tokens.shift();
  check_eol(tk, 'Expected line break');
  tk = tokens.shift();
  check_indent(tk, "Expected indent");
  A_CONFIG(tokens, edge_name, true);
  tk = tokens.shift();
  check_meta_tag(tk, "Expected 'destination:' definition", 'destination');
  tk = tokens.shift();
  check_semicolon(tk);
  tk = tokens.shift();
  check_type(tk, 'text', 'Invalid destination');
  A_ID(tk, 'Invalid destination identifier');
}

function A_ID(tk, error_msg) {
  let dag_name = tk.value.trim();
  let valid_name = /^[a-zA-Z][\w]*$/.test(dag_name);
  if (!valid_name) {
    let invalid_characters = dag_name.replace(/^[a-zA-Z][\w]*$/g, '');
    push_error_msg(error_msg, tk.line);
    throw new AnalysisException();
  }
}
