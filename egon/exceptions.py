class MissingConnectionError(Exception):
    """Raised when a connector is left unconnected at pipeline runtime"""


class MalformedSourceError(Exception):
    """Raised when a ``Source`` object is created with the wrong type of connectors"""


class MalformedTargetError(Exception):
    """Raised when a ``Target`` object is created with the wrong type of connectors"""


class OrphanedNodeError(Exception):
    """Raised when a ``Node`` pipeline is inaccessible by the pipleine due to missing connectors"""
