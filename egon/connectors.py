from __future__ import annotations

import multiprocessing as mp
from typing import Any, Optional, Union

from .nodes import Inline, Node, Source, Target


class DataStore:
    """Stores data in memory as it transfers between pipeline nodes"""

    def __init__(self) -> None:
        """Queue-like object for passing data between nodes and / or parallel processes"""

        self._queue = mp.Queue()

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
        self._node: Optional[Node] = None  # The _node that this connector is assigned to
        self._partner_connection: Optional[Connector] = None  # The connector object of another node

    def is_connected(self) -> bool:
        """Return whether the connector has an established connection"""

        return not (self._partner_connection is None)

    def connect(self, connector: Connector, maxsize: int = 0) -> None:
        """Establish the flow of data between this connector and another connector

        Args:
            connector: The connector object ot connect with
            maxsize: The maximum number of communicated items to store in memory
        """

        if type(connector) is type(self):
            raise ValueError('Cannot join together two connection objects of the same type.')

        if connector.is_connected():
            raise ValueError('Connector object already has a pre-established connection.')

        # Once a connection is established between two connectors, they share an internal queue
        self._partner_connection = connector
        connector._partner_connection = self
        connector._queue = self._queue = mp.Queue(maxsize)

    def disconnect(self) -> None:
        """Disconnect any established connections"""

        old_partner = self._partner_connection
        self._partner_connection = None
        self._queue = mp.Queue()

        if old_partner:
            old_partner.disconnect()


class Input(Connector):
    """Handles the input of data into a pipeline node"""

    def get(self, block=True, timeout=None) -> Any:
        """Retrieve data from the connector"""

        return self._queue.get(block, timeout)

    @property
    def source(self) -> Optional[Union[Source, Inline]]:
        """The Node feeding into this connection

        Returns ``None`` if no connection has been established
        """

        return self._partner_connection._node


class Output(Connector):
    """Handles the output of data from a pipeline node"""

    def put(self, x) -> None:
        """Add data into the connector"""

        self._queue.put(x)

    @property
    def destination(self) -> Optional[Union[Target, Inline]]:
        """The Node receiving from this connection

        Returns ``None`` if no connection has been established
        """

        return self._partner_connection._node
