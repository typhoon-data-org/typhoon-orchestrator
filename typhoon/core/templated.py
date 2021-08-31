import inspect
import re
import textwrap

import jinja2
from typing import Callable, List
from typing_extensions import runtime_checkable, Protocol


@runtime_checkable
class Templated(Protocol):
    template: str
    _filters: List[Callable] = None

    def get_properties(self) -> dict:
        base_properties = [
            x[0] for x in inspect.getmembers(Templated, lambda o: isinstance(o, property))
        ]
        return {
            x[0]: getattr(self, x[0])
            for x in inspect.getmembers(self.__class__, lambda o: isinstance(o, property))
            if x[0] not in base_properties
        }

    def get_methods(self) -> dict:
        base_methods = [x[0] for x in inspect.getmembers(Templated, inspect.isfunction)]
        return {
            x[0]: x[1]
            for x in inspect.getmembers(self.__class__, inspect.isfunction)
            if x[0] not in base_methods and not x[0].startswith('_')
        }

    @property
    def context(self):
        _args = {
            k: getattr(self, k).rendered if isinstance(getattr(self, k), Templated) else getattr(self, k)
            for k, v in self.__annotations__.items() if k != 'template'
        }
        return dict(_args={k: v for k, v in _args.items() if v is not None}, **_args, **self.get_properties())

    @property
    def environment(self):
        def _make_filter(method_name):
            def _custom_filter(*args):
                return getattr(self, method_name)(*args)
            return _custom_filter
            # return lambda *args: getattr(self, method_name)(*args)
        env = jinja2.Environment(
            loader=jinja2.loaders.BaseLoader,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
        for name in self.get_methods().keys():
            env.filters[name] = _make_filter(name)
        if self._filters:
            for filter_func in self._filters:
                env.filters[filter_func.__name__] = filter_func
        return env

    def expand_template(self) -> str:
        template = textwrap.dedent(self.template).strip()
        replaced = re.sub(
            r'( *){%\s*when\s+(.*)\s*%}(.*){%\s*endwhen\s*%}',
            '{% if \\g<2> is not none %}\n\\g<1>\\g<3>\n\\g<1>{% endif %}',
            template,
        )
        return re.sub(r'(\s*){%\s*when\s+(.*)\s*%}', '{% if \\g<2> is not none %}\n\\g<1>{{ \\g<2> }}\n\\g<1>{% endif %}', replaced)

    def render(self) -> str:
        return self.environment.from_string(self.expand_template()).render(self.context)

    @property
    def rendered(self) -> str:
        return self.render()

    def __str__(self) -> str:
        return self.rendered
