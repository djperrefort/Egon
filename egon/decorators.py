"""Function decorators from the ``decorators`` module provide a shorthand for
creating pipeline nodes from pre-built functions.
"""

import inspect
from typing import Any, Callable, Generator

from boltons.funcutils import wraps

from . import connectors, nodes

GeneratorFunction = Callable[[], Generator]


def _as_single_arg_func(func):
    """Return a function as a callable that accepts at most one argument"""

    if len(inspect.getfullargspec(func).args) <= 1:
        return func

    return lambda args: func(*args)


def as_source(func: GeneratorFunction) -> nodes.Source:
    """Decorator for wrapping a callable as a pipeline ``Source`` object"""

    class WrappedSource(nodes.Source):
        output = connectors.Output()

        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

        def action(self) -> None:
            for x in func():
                self.output.put(x)

    return WrappedSource()


def as_target(func: callable) -> nodes.Target:
    """Decorator for wrapping a callable as a pipeline ``Target`` object"""

    class WrappedTarget(nodes.Target):
        input = connectors.Input()

        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

        def action(self) -> None:
            single_arg_callable = _as_single_arg_func(func)
            for data in self.input.iter_get():
                single_arg_callable(data)

    return WrappedTarget()


def as_node(func: callable) -> nodes.Node:
    """Decorator for wrapping a callable as a pipeline ``Node`` object"""

    class WrappedNode(nodes.Node):
        input = connectors.Input()
        output = connectors.Output()

        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

        def action(self) -> None:
            single_arg_callable = _as_single_arg_func(func)
            for data in self.input.iter_get():
                single_arg_callable(data)

    return WrappedNode()
