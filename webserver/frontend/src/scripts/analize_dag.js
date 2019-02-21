import {check_not_eof, check_not_eol, get_tokens_block, push_error_msg} from "./ace_helper";

export function A_DAG() {
  let line = A_NAME();
  // A_SCHEDULE_INTERVAL();
  // A_ACTIVE();
  // A_NODES();
  // A_EDGES();
}

function A_NAME() {
  let tokens, line;
  [tokens, line] = get_tokens_block();

  let tk = tokens.shift();
  if ((tk.type !== "meta.tag") || (tk.value !== 'name')) {
    push_error_msg("Expected 'name:' at the top of DAG definition", tk.line);
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected DAG name, not end of line");
  check_not_eof(tk, "Expected DAG name, not end of line");
  if (tk.type !== "keyword" || tk.value !== ':') {
    push_error_msg("Expected ':' after name");
  }
  tk = tokens.shift();
  check_not_eol(tk, "Expected DAG name, not end of line");
  check_not_eof(tk, "Expected DAG name, not end of file");

  return line;
}