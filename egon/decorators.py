"""Function decorators from the ``decorators`` module provide a shorthand for
creating pipeline nodes from pre-built functions.
"""

from typing import Any, Callable, Generator

from boltons.funcutils import wraps

from . import connectors, nodes

GeneratorFunction = Callable[[], Generator]


class Wrapper:
    """Base class for wrapping functions as node-like pipeline objects"""

    _func: callable

    def __init__(self, func: callable):
        super().__init__()
        self._func = func

    def action(self) -> None:
        while True:
            data = self.input_data.get()  # Retrieve the data
            if data is connectors.KillSignal:
                break

            self.func(*data)


def as_source(func: GeneratorFunction) -> nodes.Source:
    """Decorator for wrapping a callable as a pipeline ``Source`` object"""

    class WrappedSource(Wrapper, nodes.Source):
        output = connectors.Output()

        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

        def action(self) -> None:
            for x in self._func():
                self.output.put(x)

    return WrappedSource(func)


def as_target(func: callable) -> nodes.Target:
    """Decorator for wrapping a callable as a pipeline ``Target`` object"""

    class WrappedTarget(Wrapper, nodes.Target):
        input = connectors.Input()

        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

    return WrappedTarget(func)


def as_node(func: callable) -> nodes.Node:
    """Decorator for wrapping a callable as a pipeline ``Node`` object"""

    class WrappedNode(Wrapper, nodes.Node):
        input = connectors.Input()
        output = connectors.Output()

        @staticmethod
        @wraps(func)
        def __call__(*args, **kwargs) -> Any:
            return func(*args, **kwargs)

    return WrappedNode(func)
