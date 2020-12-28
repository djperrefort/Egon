"""Function decorators from the ``decorators`` module provide a shorthand for
creating pipeline nodes from pre-built functions. Different decorators
produce different kinds of pipeline nodes.
"""

import inspect
from typing import Any, Callable, Generator

from boltons.funcutils import wraps

from . import connectors, nodes

GeneratorFunction = Callable[[], Generator]


def _as_single_arg_func(func: callable) -> callable:
    """Wrap a callable so that it accepts at most one argument

    Args:
        func: The callable to wrap

    Returns:
        The wrapped function
    """

    if len(inspect.getfullargspec(func).args) <= 1:
        return func

    return lambda args: func(*args)


class WrappedSource(nodes.Source):
    """Wrapped callable as a Source-like object"""

    def __init__(self, func: GeneratorFunction) -> None:
        self.output = connectors.Output()
        self._func = _as_single_arg_func(func)
        super().__init__()

    def action(self) -> None:
        """Call the wrapped generator and load results into the ``output`` connector"""

        for x in self._func():
            self.output.put(x)

    def __repr__(self) -> str:  # pragma: no cover
        return f'<WrappedSource(wrapped_function={self._func.__name__}) object at {hex(id(self))}>'


class WrappedTarget(nodes.Target):
    """Wrapped callable as a Target-like object"""

    def __init__(self, func: callable) -> None:
        self.input = connectors.Input()
        self._func = _as_single_arg_func(func)
        super().__init__()

    def action(self) -> None:
        """Call the wrapped function using data from the ``input`` connector"""

        for data in self.input.iter_get():
            self._func(data)

    def __repr__(self) -> str:  # pragma: no cover
        return f'<WrappedTarget(wrapped_function={self._func.__name__}) object at {hex(id(self))}>'


class WrappedNode(nodes.Node):
    """Wrapped callable as a Node-like object"""

    def __init__(self, func: callable) -> None:
        self.input = connectors.Input()
        self.output = connectors.Output()
        self._func = _as_single_arg_func(func)
        super().__init__()

    def action(self) -> None:
        """Call the wrapped function and put it's return in the ``output`` connector."""

        for data in self.input.iter_get():
            self.output.put(self._func(data))

    def __repr__(self) -> str:  # pragma: no cover
        return f'<WrappedNode(wrapped_function={self._func.__name__}) object at {hex(id(self))}>'


def as_source(func: GeneratorFunction) -> nodes.Source:
    """Decorator for wrapping a callable as a pipeline ``Source`` object"""

    class Wrapper(WrappedSource):
        @staticmethod
        @wraps(func)
        def __call__() -> Any:
            return func()

    return Wrapper(func)


def as_target(func: callable) -> nodes.Target:
    """Decorator for wrapping a callable as a pipeline ``Target`` object"""

    class Wrapper(WrappedTarget):
        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

    return Wrapper(func)


def as_node(func: callable) -> nodes.Node:
    """Decorator for wrapping a callable as a pipeline ``Node`` object"""

    class Wrapper(WrappedNode):
        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

    return Wrapper(func)
