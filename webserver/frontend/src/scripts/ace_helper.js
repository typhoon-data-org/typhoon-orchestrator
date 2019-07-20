import {A_DAG} from "./analize_dag";

export function AnalysisException() {}

let errors = [];
export function firstSyntaxError() {
  if (errors.length > 0) {
    return errors[0].text;
  }
  return '';
}
let editor;
let INDENT_TK = () => ({type: 'special', value: '__INDENT__'});
let DEDENT_TK = () => ({type: 'special', value: '__DEDENT__'});
let LINEBREAK_TK = () => ({type: 'special', value: '__LINEBREAK__'});
let EOF_TK = () => ({type: 'special', value: '__EOF__'});

export function push_error_msg(message, line) {
  let error = {
    row: line,   // zero based
    //column: error_token['col'],
    text: message,
    type: "error"
  };
  errors.push(error);
}

export function push_warning_msg(message, line) {
  let error = {
    row: line,   // zero based
    //column: error_token['col'],
    text: message,
    type: "warning"
  };
  errors.push(error);
}

// exports.syntactical_analysis = function(editor) {
export function syntactical_analysis(_editor) {
  errors = [];
  editor = _editor;
  let success = A_DAG();

  editor.session.setAnnotations(errors);

  return success
}
// }

export function get_tokens(line, previous_indentation=0) {
  if (line >= editor.session.getLength()) {
    return [[EOF_TK], -1];
  } else if (editor.session.getLine(line).trim() === '') {
    return [[], -2];
  }

  let tokens = [];
  let line_tokens = editor.session.getTokens(line);
  // while (line_tokens === []) {
  //   line++;
  //   line_tokens = editor.session.getTokens(line);
  // }
  let indents = get_indentation(line);
  if (indents - previous_indentation > 0) {
    tokens = tokens.concat(
      enrich_tokens(Array(indents - previous_indentation).fill(INDENT_TK()), line)
    );
  } else if (previous_indentation - indents > 0) {
    tokens = tokens.concat(
      enrich_tokens(Array(previous_indentation - indents).fill(DEDENT_TK()), line)
    );
  }
  tokens = tokens.concat(enrich_tokens(line_tokens, line));
  let lb_tk = LINEBREAK_TK();
  lb_tk.line = line;
  tokens.push(lb_tk);
  return [tokens, indents];
}

// Add line and trim tokens, remove empty space ones
function enrich_tokens(tokens, line) {
  return tokens.map(tk => {
    tk.line = line;
    return tk;
  }).filter(tk => (tk.value !== ' ') && (tk.value !== ''));
}

function get_indentation(line) {
  let num_spaces = editor.session.getLine(line).search(/\S|$/);
  if (num_spaces % 2 !== 0)
    push_error_msg('Even number of spaces required: got ' + num_spaces);

  return Math.floor(num_spaces/2);
}

function empty_line(tokens) {
  let i = 0;
  let tk = tokens[i];
  while (is_indent(tk) || is_dedent(tk)) {
    i++;
    tk = tokens[i];
  }
  return is_eol(tk);
}

export function get_tokens_block(line = 0) {
  let tokens = [];
  let line_tokens, indents;
  [line_tokens, indents] = get_tokens(line);
  while (indents === -2) {
    line++;
    [line_tokens, indents] = get_tokens(line);
  }
  if (indents !== 0) {
    push_error_msg('Block should start without indentation. Found ' + indents, line);
    throw new AnalysisException();
  }
  tokens = tokens.concat(line_tokens);

  line++;
  [line_tokens, indents] = get_tokens(line, indents);
  while (indents === -2) {
    line++;
    [line_tokens, indents] = get_tokens(line);
  }
  while (line < editor.session.getLength() && (indents > 0 || editor.session.getLine(line).trim() === '')) {
    // if (empty_line(line_tokens, indents)) {
    //   line_tokens = line_tokens.filter(tk => !is_eol(tk));
    // }
    if (!(editor.session.getLine(line).trim() === '')){
      tokens = tokens.concat(line_tokens);
    }
    line++;
    [line_tokens, indents] = get_tokens(line, indents);
    while (indents === -2) {
      line++;
      [line_tokens, indents] = get_tokens(line);
    }
  }
  if (indents === -1) {
    let eof_tk = EOF_TK();
    eof_tk.line = line;
    tokens.push(eof_tk);
  }

  return [tokens, line];
}

export function check_not_eol(tk, msg) {
  if ((tk.type === 'special') && (tk.value === LINEBREAK_TK().value)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function check_not_eof(tk, msg) {
  if ((tk.type === 'special') && (tk.value === EOF_TK().value)) {
    push_error_msg(msg, tk.line - 1);
    throw new AnalysisException();
  }
}

export function check_eol(tk, msg) {
  if ((tk.type !== 'special') || (tk.value !== LINEBREAK_TK().value)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function is_eol(tk) {
  return (tk.type === 'special') && (tk.value === LINEBREAK_TK().value);
}

export function is_indent(tk) {
  return (tk.type === 'special') && (tk.value === INDENT_TK().value);
}

export function is_dedent(tk) {
  return (tk.type === 'special') && (tk.value === DEDENT_TK().value);
}

export function is_eof(tk) {
  return (tk.type === 'special') && (tk.value === EOF_TK().value);
}

export function check_eof(tk, msg) {
  if ((tk.type !== 'special') || (tk.value !== EOF_TK().value)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function check_indent(tk, msg) {
  if ((tk.type !== 'special') || (tk.value !== INDENT_TK().value)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function check_dedent(tk, msg) {
  if ((tk.type !== 'special') || (tk.value !== DEDENT_TK().value)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function check_meta_tag(tk, msg, value = undefined) {
  if ((tk.type !== "meta.tag") || ((value !== undefined) && (tk.value.trim() !== value))) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function check_semicolon(tk) {
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':' after name", tk.line);
    throw new AnalysisException();
  }
}

export function check_type(tk, type, msg) {
  if (typeof type === 'string' || type instanceof String) {
    type = [type];
  }
  if (!type.includes(tk.type)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function is_type(tk, type) {
  if (typeof type === 'string' || type instanceof String) {
    type = [type];
  }
  return type.includes(tk.type);
}

export function is_type_value(tk, type, value) {
  if (typeof type === 'string' || type instanceof String) {
    type = [type];
  }
  if (value instanceof  RegExp) {
    return type.includes(tk.type) && value.test(tk.value.trim());
  }
  return type.includes(tk.type) && (tk.value.trim() === value);
}

export function check_type_value(tk, type, value, msg) {
  if ((tk.type !== type) || (tk.value !== value)) {
    push_error_msg(msg, tk.line);
    throw new AnalysisException();
  }
}

export function num_lines() {
  return editor.session.doc.getAllLines().length;
}

export function stringify_until_eol(tokens) {
  let str = '';
  let tk = tokens.shift();
  while (!is_eol(tk)) {
    // check_type(
    //   tk,
    //   ['text', 'string', 'constant.numeric', 'constant.language.boolean'],
    //   'Invalid type for ' + tk.value,
    // );
    str += tk.value;
    tk = tokens.shift();
  }
  let lb_tk = LINEBREAK_TK();
  lb_tk.line = tk.line;
  tokens.unshift(lb_tk);

  tokens.unshift({
    type: 'string',
    value: str,
    line: tk.line,
  });
}

export const SPECIAL_VARS = ['$BATCH', '$DAG_CONTEXT', '$BATCH_NUM', '$HOOK', '$VARIABLE, $DAG_NAME'];

export function is_valid_special_var(special_var, check_nums) {
  if (SPECIAL_VARS.includes(special_var)) {
    return true;
  }
  return check_nums && /^\$[\d]+$/.test(special_var)
}

export function skip_blank_lines(tokens) {
  while (is_indent(tokens[0]) || is_dedent(tokens[0]) || is_eol(tokens[0])) {
    tokens.shift();
  }
}

export function get_indents(line) {
  let [_, indents] = get_tokens(line);
  return indents;
}

export function copyToClipboard(str) {
  const el = document.createElement('textarea');
  el.value = str;
  document.body.appendChild(el);
  el.select();
  document.execCommand('copy');
  document.body.removeChild(el);
};

export function skip_to_next_block(line) {
  line++;
  if (line >= editor.session.getLength()) {
    push_error_msg('Expected block, found EOF', line);
    throw new AnalysisException();
  }
  return line;
}
