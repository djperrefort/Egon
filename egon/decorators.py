"""Function decorators from the ``decorators`` module provide a shorthand for
creating pipeline nodes from pre-built functions.
"""

from typing import Any, Callable, Generator

from boltons.funcutils import wraps

from . import connectors, nodes

GeneratorFunction = Callable[[], Generator]


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
            while True:
                data = self.input.get()  # Retrieve the data
                if data is connectors.KillSignal:
                    break

                func(*data)

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
            while True:
                data = self.input.get()  # Retrieve the data
                if data is connectors.KillSignal:
                    break

                func(*data)

    return WrappedNode()
