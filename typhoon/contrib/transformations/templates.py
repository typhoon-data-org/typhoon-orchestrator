import jinja2


def render(template: str, **context):
    return jinja2.Template(template).render(context)
