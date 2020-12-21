from copy import copy
from typing import Type, Union

from . import connectors, units

UpStreamUnit = Union[units.Source, units.Inline]
DownStreamUnit = Union[units.Inline, units.Target]
PipelineUnit = Union[units.Source, units.Inline, units.Target]
PipelineUnitType = Union[Type[units.Source], Type[units.Inline], Type[units.Target]]


def _create_wrapped_object(func: callable, return_type: PipelineUnitType, **attr: connectors) -> PipelineUnit:
    """Wrap a callable as a pipeline unit

    Args:
        func: The callable to wrap
        return_type: The type of object to return
        **attr: Any connections to assign to the object

    Returns:
        An instantiated pipeline unit
    """

    wrapped_process = copy(return_type)
    wrapped_process.action = func
    for key, val in attr.items():
        setattr(wrapped_process, key, val)

    return wrapped_process.__init__()


def as_source(func: callable, **targets: DownStreamUnit) -> units.Source:
    """Wrap a callable as a pipeline ``Source`` object

    Args:
        func: The callable to wrap
        **targets: Any connections to assign to the object  # Todo: Fix this doc line

    Returns:
        A callable ``Source`` object
    """

    return _create_wrapped_object(func, units.Source, **targets)


def as_target(func: callable, **sources: UpStreamUnit) -> units.Target:
    """Wrap a callable as a pipeline ``Target`` object

    Args:
        func: The callable to wrap
        **sources: Any connections to assign to the object

    Returns:
        A callable ``Target`` object
    """

    return _create_wrapped_object(func, units.Target, **sources)


def as_inline(func: callable, **connections: PipelineUnit) -> units.Inline:
    """Wrap a callable as a pipeline ``Inline`` object

    Args:
        func: The callable to wrap
        **connections: Any connectors to assign to the object

    Returns:
        A callable ``Inline`` object
    """

    return _create_wrapped_object(func, units.Inline, **connections)
