import jinja2
from typing import Union


def render(template: Union[str, jinja2.Template], **context) -> str:
    """
    Renders the given string as a jinja template with the keyword arguments as context.
    :param template: String to be used as jinja template
    :param context: keyword arguments used as context for the jinja template
    :return: Rendered string
    """
    if isinstance(template, str):
        template = jinja2.Template(template)
    return template.render(context)
