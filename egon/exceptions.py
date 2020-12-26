"""The ``exceptions`` class defines custom exceptions raised by the ``egon`` package."""


class MissingConnectionError(Exception):
    """Raised when a connector is left unconnected at pipeline runtime."""


class MalformedSourceError(Exception):
    """Raised when a ``Source`` object is created with the wrong type of connectors."""


class MalformedTargetError(Exception):
    """Raised when a ``Target`` object is created with the wrong type of connectors."""


class OrphanedNodeError(Exception):
    """Raised when a ``Node`` is inaccessible by the pipeline due to missing connectors."""


class OverwriteConnectionError(Exception):
    """Raised when trying to overwrite an existing connection between two connectors."""
