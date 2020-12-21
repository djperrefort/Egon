from copy import copy
from typing import Union, Type

from simple_pipe import connectors, nodes


def _create_wrapper(return_type: Type[nodes.NodeBase], **attr: connectors) -> callable:
    """Wrap a callable as a pipeline node

    Args:
        return_type: The type of object to return
        **attr: Any connector objects to assign to the wrapped function

    Returns:
        An instantiated pipeline node
    """

    def wrapper(func: callable) -> nodes.NodeBase:
        wrapped_process = copy(return_type)
        wrapped_process.action = func
        for key, val in attr.items():
            setattr(wrapped_process, key, val)

        return wrapped_process()

    return wrapper


def as_source(**connections: connectors.Output) -> callable:
    """Wrap a callable as a pipeline ``Source`` object

    Args:
        **connections: Any connector objects to assign to the wrapped function

    Returns:
        A wrapper for casting a function as a callable ``Source`` object
    """

    return _create_wrapper(nodes.Source, **connections)


def as_target(**connections: connectors.Input) -> callable:
    """Wrap a callable as a pipeline ``Target`` object

    Args:
        **connections: Any connector objects to assign to the wrapped function

    Returns:
        A wrapper for casting a function as a callable ``Target`` object
    """

    return _create_wrapper(nodes.Target, **connections)


def as_inline(**connections: Union[connectors.Input, connectors.Output]) -> callable:
    """Wrap a callable as a pipeline ``Inline`` object

    Args:
        **connections: Any connector objects to assign to the wrapped function

    Returns:
        A wrapper for casting a function as a callable ``Inline`` object
    """

    return _create_wrapper(nodes.Inline, **connections)
