import {get_parents} from "./completer";

export function get_docobject(line, session, pos) {
  if (/functions\.\w+\.\w+/.test(line)) {
    let r = /functions\.(\w+)\.(\w+)/.exec(line);
    let module = r[1];
    let name = r[2];
    return {
      'type': 'user_function',
      'module': module,
      'name': name
    }
  } else if (/transformations\.\w+\.\w+/.test(line)) {
    let r = /transformations\.(\w+)\.(\w+)/.exec(line);
    let module = r[1];
    let name = r[2];
    return {
      'type': 'user_transformation',
      'module': module,
      'name': name
    }
  } else if (/typhoon\.\w+\.\w+/.test(line)) {
    let r = /typhoon\.(\w+)\.(\w+)/.exec(line);
    let module = r[1];
    let name = r[2];
    let parents = get_parents(session, pos);
    if (parents.length === 2 && parents[0].type === 'nodes') {
      return {
        'type': 'typhoon_function',
        'module': module,
        'name': name,
      }
    } else if (parents.length >= 2 && parents[0].type === 'edges') {
      return {
        'type': 'typhoon_transformation',
        'module': module,
        'name': name,
      }
    }
  }
  return null;
}