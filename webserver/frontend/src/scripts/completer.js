
export function get_completions(editor, session, pos, prefix) {
  if (pos.column === prefix.length) {
    return ["name", "schedule-interval", "active", "nodes", "edges"];
  }
  let line_text = session.getLine(pos.row);
  if (line_text.startsWith('schedule-interval: ') && 'rate'.includes(prefix)) {
    return ['rate()'];
  }
  return [];
}