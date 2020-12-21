from __future__ import annotations

import multiprocessing as mp
from typing import Any, Optional

from .nodes import Node


class DataStore:
    """Stores data in memory as it transfers between pipeline nodes"""

    def __init__(self, maxsize: int = 0) -> None:
        """Queue-like object for passing data between nodes and / or parallel processes

        Args:
            maxsize: The maximum number of items that can be stored in the queue
        """

        self._maxsize = maxsize
        self._queue = mp.Queue(maxsize)

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

    def __init__(self, maxsize) -> None:
        """Handles the communication of input/output data between pipeline nodes

        Args:
            maxsize: The maximum number of communicated objects that can be stored in memory at once
        """

        super().__init__(maxsize)
        self._node: Optional[Node] = None  # The _node that this connector is assigned to
        self._partner: Optional[Connector] = None  # The connector object of another _node

    def is_connected(self) -> bool:
        """Return whether the connector has an established connection"""

        return not (self._partner is None)

    def connect(self, connector: Connector) -> None:
        """Establish the flow of data between this connector and another connector

        Args:
            connector: The connector object ot connect with
        """

        if type(connector) is type(self):
            raise ValueError('Cannot join together two connection objects of the same type.')

        if connector.is_connected():
            raise ValueError('Connector object already has a pre-established connection.')

        self._partner = connector
        connector._partner = self
        connector._queue = self._queue

    def disconnect(self) -> None:
        """Disconnect any established connections"""

        old_partner = self._partner
        self._partner = None
        self._queue = mp.Queue(maxsize=self._maxsize)

        if old_partner:
            old_partner.disconnect()


class Input(Connector):
    """Handles the input of data into a pipeline node"""

    def get(self, block=False, timeout=None) -> Any:
        """Retrieve data from the connector"""

        return self._queue.get(block, timeout)

class Output(Connector):
    """Handles the output of data from a pipeline node"""

    def put(self, x) -> None:
        """Add data into the connector"""

        self._queue.put(x)
