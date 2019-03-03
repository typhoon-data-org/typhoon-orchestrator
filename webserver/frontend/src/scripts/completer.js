import {SPECIAL_VARS} from "./ace_helper";

let TYPHOON_MODULES = [];
let TYPHOON_FUNCTIONS = {};

let CUSTOM_MODULES = [];
let CUSTOM_FUNCTIONS = {};

function is_beginning_line(pos, prefix) {
  return pos.column === prefix.length;
}

export function get_completions(editor, session, pos, prefix, typhoonModules, typhoonFunctions, userDefinedModules, userDefinedFunctions) {
  TYPHOON_MODULES = typhoonModules;
  TYPHOON_FUNCTIONS = typhoonFunctions;
  CUSTOM_MODULES = userDefinedModules;
  CUSTOM_FUNCTIONS = userDefinedFunctions;

  if (is_beginning_line(pos, prefix)) {
    return ["name", "schedule-interval", "active", "nodes", "edges"];
  }

  let line_text = session.getLine(pos.row);
  if (line_text.startsWith('schedule-interval: ') && 'rate'.includes(prefix)) {
    return ['rate()'];
  }

  let parents = get_parents(session, pos);
  if (parents.length > 1 && parents[0].type === 'nodes') {
    return get_completions_node(editor, session, pos, prefix, parents);
  }
  if (parents.length > 1 && parents[0].type === 'edges') {
    // return get_completions_edge(editor, session, pos, prefix, parents);
  }
  return [];
}

function get_completions_node(editor, session, pos, prefix, parents) {
  let line_text = session.getLine(pos.row);
  let indents = line_indentation(line_text);
  if (indents === 2 && '    function'.includes(line_text)) {
    return ['function:'];
  } else if (indents === 2 && '    config: '.includes(line_text)) {
    return ['config:'];
  } else if (indents === 2 && ('    function: typhoon'.includes(line_text) || '    function: functions'.includes(line_text))) {
    return ['typhoon', 'functions'];
  } else if (indents === 2 && /^ {4}function: typhoon\.([^.]+)(\.([^.]*$))/.test(line_text)) {
    let typhoon_module = /^ {4}function: typhoon\.([^.]+)(\.([^.]*))/.exec(line_text)[1];
    return TYPHOON_FUNCTIONS[typhoon_module];
  } else if (indents === 2 && /^ {4}function: typhoon\.([^.]*$)/.test(line_text)) {
    return TYPHOON_MODULES;
  } else if (indents === 2 && /^ {4}function: functions\.([^.]+)(\.([^.]*$))/.test(line_text)) {
    let custom_module = /^ {4}function: functions\.([^.]+)(\.([^.]*))/.exec(line_text)[1];
    return CUSTOM_FUNCTIONS[custom_module];
  } else if (indents === 2 && /^ {4}function: functions\.([^.]*$)/.test(line_text)) {
    return CUSTOM_MODULES;
  } else if (indents === 3 && /^ {6}[^: ]+$/.test(line_text)) {
    let config_name = /^ {6}([^: ]+)$/.exec(line_text)[1];
    return [config_name + ' => APPLY'];
  } else if (indents === 4 && (pos.column - prefix.length - 12) > 8 &&
    line_text.slice(pos.column - prefix.length - 12, pos.column - prefix.length) === '$DAG_CONFIG.') {
    return ['ds', 'ds_nodash', 'execution_date'];
  } else if (indents === 4 && prefix.startsWith('$')) {
    return SPECIAL_VARS;
  }
  return [];
}

function line_indentation(line_text) {
  let num_spaces = line_text.search(/\S|$/);
  if (num_spaces % 2 !== 0)
    return -1;
  else
    return Math.floor(num_spaces/2);
}

function get_parents(session, pos) {
  let line_text = session.getLine(pos.row);
  let line_indents = line_indentation(line_text);
  if (line_indents < 1)
    return [];

  let parents = [];
  let line_num = pos.row;
  let parent_indents = line_indents - 1;
  for (let i = line_num - 1; i >= 0; i--) {
    let previous_line_text = session.getLine(i);
    let previous_line_indents = line_indentation(previous_line_text);

    if (/^\s*$/.test(previous_line_text))
      continue;
    if (parent_indents > 2) {
      parent_indents --;
    } else if (previous_line_indents === parent_indents) {
      if (parent_indents === 2 && previous_line_text.startsWith('    config:')) {
        parents.push({type: 'config', value: 'config'});
      } else if (parent_indents === 2 && previous_line_text.startsWith('    adapter:')) {
        parents.unshift({type: 'config', value: 'adapter'});
      }

      else if (parent_indents === 1 && /^ {2}\w*:/.test(previous_line_text)) {
        parents.unshift({type: 'node_or_edge', value: /^ {2}(\w*):/.exec(previous_line_text)[1]});
      }

      else if (parent_indents === 0 && previous_line_text.startsWith('nodes:')) {
        parents.unshift({type: 'nodes', value: 'nodes'});
        break;
      } else if (parent_indents === 0 && previous_line_text.startsWith('edges:')) {
        parents.unshift({type: 'edges', value: 'edges'});
        break;
      }
      else {
        return [];
      }
      parent_indents--;

    }
  }

  if (parents.length > 0 && !['nodes', 'edges'].includes(parents[0].type)) {
    return [];
  }
  if (parents.length > 1 && parents[1].type !== 'node_or_edge'){
    return [];
  }
  if (parents.length > 2 && parents[2].type !== 'config'){
    return []
  }

  return parents;
}
