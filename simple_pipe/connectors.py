from __future__ import annotations

import multiprocessing as mp
from functools import wraps
from typing import Any, Optional

from .nodes import NodeBase


class ConnectorBase:
    """Base class for all connection objects"""

    def __init__(self, maxsize: int = 0) -> None:
        """Queue-like object that handles the passing of data between pipeline nodes

        Args:
            maxsize: The maximum number of items that can be stored in the queue
        """

        self._queue = mp.Queue(maxsize)  # Handles the passing of data between nodes
        self.node: Optional[NodeBase] = None  # The node that this connector is assigned to
        self._partner: Optional[ConnectorBase] = None  # The connector object of another node

    def connected(self) -> bool:
        """Return whether the connector has been connected to a task"""

        return not (self._partner is None)

    def connect(self, connector) -> None:
        """Establish the flow of data between this connector and a partner

        Args:
            connector: The connector object ot connect with
        """

        if type(connector) is type(self):
            raise ValueError('Cannot join together two connection objects of the same type.')

        if connector.connected():
            raise ValueError('Connector object already has a pre-established connection.')

        self._partner = connector
        connector._partner = self
        connector._queue = self._queue

    def disconnect(self) -> None:
        """Disconnect the connector from its partner"""

        old_partner = self._partner
        self._partner = None
        self._queue = None

        if old_partner:
            old_partner.disconnect()


class Input(ConnectorBase):
    """Handles the input of data into a pipeline node"""

    @wraps(ConnectorBase.connect)
    def connect(self, connector: Output) -> None:
        super(Input, self).connect(connector)

    def get(self) -> Any:
        """Retrieve data from the connector"""

        return self._queue.get()


class Output(ConnectorBase):
    """Handles the output of data from a pipeline node"""

    @wraps(ConnectorBase.connect)
    def connect(self, connector: Input) -> None:
        super(Output, self).connect(connector)

    def put(self, x) -> None:
        """Add data into the connector"""

        self._queue.put(x)
