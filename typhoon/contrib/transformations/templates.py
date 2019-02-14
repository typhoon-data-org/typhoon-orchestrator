import jinja2


def render(s: str, **context):
    return jinja2.Template(s).render(context)
