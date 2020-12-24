"""Function decorators from the ``decorators`` module provide a shorthand for
creating pipeline nodes from pre-built functions.
"""

from typing import Any, Callable, Generator

from . import connectors, nodes

GeneratorFunction = Callable[[], Generator]


class Wrapper:
    """Base class for wrapping functions as node-like pipeline objects"""

    _func: callable

    def __init__(self, func: callable):
        super().__init__()
        self._func = func

    def __call__(self, *args, **kwargs) -> Any:
        return self._func(*args, **kwargs)

    def action(self) -> None:
        while self.expecting_inputs():
            data = self.input.get()
            self._func(*data)


class WrappedSource(Wrapper, nodes.Source):
    output = connectors.Output()

    def action(self) -> None:
        for x in self._func():
            self.output.put(x)


class WrappedTarget(Wrapper, nodes.Target):
    input = connectors.Input()


class WrappedNode(Wrapper, nodes.Node):
    input = connectors.Input()
    output = connectors.Output()


def as_source(func: GeneratorFunction) -> WrappedSource:
    """Decorator for wrapping a callable as a pipeline ``Source`` object"""

    return WrappedSource(func)


def as_target(func: callable) -> WrappedTarget:
    """Decorator for wrapping a callable as a pipeline ``Target`` object"""

    return WrappedTarget(func)


def as_node(func: callable) -> WrappedNode:
    """Decorator for wrapping a callable as a pipeline ``Node`` object"""

    return WrappedNode(func)
