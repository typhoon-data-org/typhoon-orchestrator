import jinja2


def render(template: str, **context) -> str:
    """
    Renders the given string as a jinja template with the keyword arguments as context.
    :param template: String to be used as jinja template
    :param context: keyword arguments used as context for the jinja template
    :return: Rendered string
    """
    return jinja2.Template(template).render(context)
