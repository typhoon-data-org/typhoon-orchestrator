import {A_DAG} from "./analize_dag";

let errors = [];
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
  A_DAG();

  editor.session.setAnnotations(errors);

  let tokens, line;
  [tokens, line] = get_tokens_block();
  return tokens
}
// }

function get_tokens(line, previous_indentation=0) {
  if (line >= editor.session.getLength()) {
    return [[EOF_TK], -1];
  }

  let tokens = [];
  let line_tokens = editor.session.getTokens(line);
  while (line_tokens === []) {
    line++;
    line_tokens = editor.session.getTokens(line);
  }
  let indents = get_indentation(line);
  if (indents - previous_indentation > 0) {
    tokens = tokens.concat(
      add_line_tokens(Array(indents - previous_indentation).fill(INDENT_TK()), line)
    );
  } else if (previous_indentation - indents > 0) {
    tokens = tokens.concat(
      add_line_tokens(Array(previous_indentation - indents).fill(DEDENT_TK()), line)
    );
  }
  tokens = tokens.concat(add_line_tokens(line_tokens, line));
  let lb_tk = LINEBREAK_TK();
  lb_tk.line = line;
  tokens.push(lb_tk);
  return [tokens, indents];
}

function add_line_tokens(tokens, line) {
  return tokens.map(tk => {
    tk.line = line;
    return tk;
  });
}

function get_indentation(line) {
  let num_spaces = editor.session.getLine(line).search(/\S/);
  if (num_spaces % 2 !== 0)
    push_error_msg('Even number of spaces required: got ' + num_spaces)

  return Math.floor(num_spaces/2);
}

export function get_tokens_block(line = 0) {
  let tokens = [];
  let line_tokens, indents;
  [line_tokens, indents] = get_tokens(line);
  if (indents !== 0)
    push_error_msg('Block should start without indentation. Found ' + indents);
  tokens = tokens.concat(line_tokens);

  line++;
  [line_tokens, indents] = get_tokens(line, indents);
  while (indents > 0) {
    tokens = tokens.concat(line_tokens);
    line++;
    [line_tokens, indents] = get_tokens(line, indents);
  }
  if (indents === -1)
    tokens.push(EOF_TK());

  return [tokens, line];
}

export function check_not_eol(tk, msg) {
  if ((tk.type === 'special') && (tk.value === LINEBREAK_TK().value)) {
    push_warning_msg(msg, tk.line);
  }
}

export function check_not_eof(tk, msg) {
  if ((tk.type === 'special') && (tk.value === EOF_TK().value)) {
    push_warning_msg(msg, tk.line);
  }
}

// exports.get_all_tokens = function(editor) {
//   let num_lines = editor.session.getLength();
//   let tokens = [];
//   for (let i = 0; i < num_lines; i++) {
//     let tokens_line = editor.session.getTokens(i);
//     tokens = tokens.concat(tokens_line);
//   }
//   return tokens;
// };

// module.exports = {
//   syntactical_analysis: syntactical_analysis,
//   get_tokens_block: get_tokens_block
// };
