from __future__ import annotations

import multiprocessing as mp
from typing import Any, Optional, TYPE_CHECKING, Union

from . import exceptions

if TYPE_CHECKING:  # pragma: no cover
    from .nodes import Node, AbstractNode, Source, Target


class KillSignal:
    """Used to indicate that a process should exit"""


class DataStore:
    """Exposes select functionality from an underlying ``Queue`` object"""

    def __init__(self, maxsize=0) -> None:
        """Queue-like object for passing data between nodes and / or parallel processes

        Args:
            maxsize: Maximum number of objects to store in memory
        """

        self._queue = mp.Queue(maxsize=maxsize)

    def empty(self) -> bool:
        """Return if the connection queue is empty"""

        return self._queue.empty()

    def full(self) -> bool:
        """Return if the connection queue is full"""

        return self._queue.full()

    def size(self) -> int:
        """Return the size of the connection queue"""

        return self._queue.qsize()


class Connector(DataStore):
    """Base class for signal/slot like objects"""

    def __init__(self) -> None:
        """Handles the communication of input/output data between pipeline nodes"""

        super().__init__()
        self._node: Optional[AbstractNode] = None  # The node that this connector is assigned to
        self._connected_partner: Optional[Connector] = None  # The connector object of another node

    def is_connected(self) -> bool:
        """Return whether the connector has an established connection"""

        return not (self._connected_partner is None)

    def connect(self, connector: Connector, maxsize: int = 0) -> None:
        """Establish the flow of data between this connector and another connector

        Args:
            connector: The connector object ot connect with
            maxsize: The maximum number of communicated items to store in memory
        """

        if type(connector) is type(self):
            raise ValueError('Cannot join together two connection objects of the same type.')

        if connector.is_connected() or self.is_connected():
            raise exceptions.OverwriteConnectionError(
                'Connector object already has a pre-established connection.')

        # Once a connection is established between two connectors, they share an internal queue
        self._connected_partner = connector
        connector._connected_partner = self
        connector._queue = self._queue = mp.Queue(maxsize)

    def disconnect(self) -> None:
        """Disconnect any established connections"""

        old_partner = self._connected_partner
        self._connected_partner = None
        self._queue = mp.Queue()

        if old_partner:
            old_partner.disconnect()


class Input(Connector):
    """Handles the input of data into a pipeline node"""

    def get(self, timeout: Optional[int] = None, refresh_interval: int = 10):
        """Blocking call to retrieve input data

        Releases automatically when no more data is coming from upstream

        Args:
            timeout: Raise a TimeoutError if data is not retrieved within the given number of seconds
            refresh_interval: How often to check if data is expected from upstream

        Raises:
            TimeOutError: Raised if the get call times out
        """

        if not refresh_interval > 0:
            raise ValueError('Connector refresh interval must be greater than zero.')

        remaining_time = timeout
        while remaining_time > 0:
            timeout_interval = min(abs(remaining_time - timeout), timeout)

            if self.source_node.node_finished:
                return KillSignal

            try:
                return self.get(timeout=timeout_interval)

            except Exception as e:
                remaining_time -= timeout_interval

        raise e

    @property
    def source_connector(self) -> Connector:
        """The connector object connected to this instance

        Returns ``None`` if no connection has been established
        """

        return self._connected_partner

    @property
    def source_node(self) -> Optional[Union[Source, Node]]:
        """The connected pipeline node feeding into this connection

        Returns ``None`` if no connection has been established
        """

        return self.source_connector._node


class Output(Connector):
    """Handles the output of data from a pipeline node"""

    def put(self, x: Any) -> None:
        """Add data into the connector"""

        self._queue.put(x)

    @property
    def destination_connector(self) -> Connector:
        """The connector object connected to this instance

        Returns ``None`` if no connection has been established
        """

        return self._connected_partner

    @property
    def destination_node(self) -> Optional[Union[Target, Node]]:
        """The connected pipeline node receiving data from this connector

        Returns ``None`` if no connection has been established
        """

        return self.destination_connector._node
