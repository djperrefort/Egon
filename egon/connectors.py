"""``Connector`` objects are responsible for the communication of
information between nodes. Each connector instance is assigned to a
single  node and can be used to send or receive data depending on the
type of connector. ``Output`` connectors are used to send data and
``Input`` objects are used to receive data.
"""

from __future__ import annotations

import abc
import multiprocessing as mp
from queue import Empty
from typing import Any, Optional, TYPE_CHECKING, Iterable, List, Collection

from . import exceptions
from .exceptions import MissingConnectionError

if TYPE_CHECKING:  # pragma: no cover
    from .nodes import AbstractNode


class KillSignal:
    """Used to indicate that a process should exit"""


class ObjectCollection:
    """Collection of objects with O(1) add and remove"""

    def __init__(self, data: Optional[Collection] = None) -> None:
        """A mutable collection of arbitrary objects

        Args:
            data: Populate the collection instance with the given data
        """

        # Map object hash values to their index in a list
        self._object_list = list(set(data)) if data else []
        self._index_map = {o: i for i, o in enumerate(self._object_list)}

    def add(self, x: Any) -> None:
        """Add a hashable object to the collection

        Args:
            x: The object to add
        """

        # Exit if ``x`` is already in the collection
        if x in self._index_map:
            return

        # Add ``x`` to the end of the collection
        self._index_map[x] = len(self._object_list)
        self._object_list.append(x)

    def remove(self, x: Any) -> None:
        """Remove an object from the collection

        Args:
            x: The object to remove
        """

        index = self._index_map[x]

        # Swap element with last element so that removal from the list can be done in O(1) time
        size = len(self._object_list)
        last = self._object_list[size - 1]
        self._object_list[index], self._object_list[size - 1] = self._object_list[size - 1], self._object_list[index]

        # Update hash table for new index of last element
        self._index_map[last] = index

        del self._index_map[x]
        del self._object_list[-1]

    def __iter__(self) -> Iterable:
        return iter(self._object_list)

    def __contains__(self, item: Any) -> bool:
        return item in self._object_list

    def __len__(self) -> int:
        return len(self._object_list)

    def __repr__(self) -> str:  # pragma: no cover
        return f'<Container({self._object_list})>'


class AbstractConnector:
    """Exposes select functionality from an underlying ``Queue`` object"""

    def __init__(self, name: str = None, maxsize: int = 0) -> None:
        """Queue-like object for passing data between nodes and / or parallel processes

        Args:
            name: Human readable name for the connector object
            maxsize: Maximum number of items to store in the underlying queue
        """

        self.name = name
        self._node: Optional[AbstractNode] = None  # The node that this connector is assigned to
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

    @property
    def parent_node(self) -> AbstractNode:
        """The parent node this connector is assigned to"""

        return self._node

    @property
    @abc.abstractmethod
    def is_connected(self) -> bool:  # pragma: no cover
        pass


class Input(AbstractConnector):
    """Handles the input of data into a pipeline node"""

    def __init__(self, name: str = None, maxsize: int = 0) -> None:
        """Handles the input of data into a pipeline node

        Args:
            name: Human readable name for the connector object
            maxsize: The maximum number of communicated items to store in memory
        """

        super().__init__(name, maxsize)
        self._connected_partners = ObjectCollection()  # Tracks connector objects that feed into the input

    @property
    def maxsize(self) -> int:
        """The maximum number of objects to store in the connector's memory

        Once the maximum size is reached, the ``put`` method will block until
        an item is moved from the connector into the node.
        """

        return self._queue._maxsize

    @maxsize.setter
    def maxsize(self, maxsize: int) -> None:
        """Replaces the underlying queue with a new instance and updated
        connected outputs to point at that new instance.
        """

        if not self.empty():
            raise RuntimeError('Cannot change maximum connector size when the connector is not empty.')

        self._queue = mp.Queue(maxsize=maxsize)
        for partner in self._connected_partners:
            partner._queue = self._queue

    @property
    def is_connected(self) -> bool:
        """Return whether the connector has any established connections"""

        return bool(self._connected_partners)

    def get_partners(self) -> List[Output]:
        """Return a list of output connectors that are connected to this input"""

        return list(self._connected_partners)

    def get(self, timeout: Optional[int] = None, refresh_interval: int = 2):
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

        timeout = timeout or float('inf')
        while timeout > 0:
            if self.is_connected and not self.parent_node.expecting_data():
                return KillSignal

            try:
                return self._queue.get(timeout=min(timeout, refresh_interval))

            except (Empty, TimeoutError):
                timeout -= refresh_interval

        raise TimeoutError

    def iter_get(self) -> Any:
        """Iterator that returns input data

        Automatically exits once no more data is expected from upstream nodes.
        """

        while self.parent_node.expecting_data():
            data = self.get()
            if data is KillSignal:
                return

            yield data

    def __repr__(self) -> str:  # pragma: no cover
        return f'<egon.connectors.Input(name={self.name}) object at {hex(id(self))}>'


class Output(AbstractConnector):
    """Handles the output of data from a pipeline node"""

    def __init__(self, name: str = None) -> None:
        """Handles the output of data from a pipeline node

        Args:
            name: Human readable name for the connector object
        """

        super().__init__(name)
        self._partner: Optional[Input] = None  # The connector object of another node

    @property
    def is_connected(self) -> bool:
        """Return whether the connector has any established connections"""

        return bool(self._partner)

    def get_partner(self) -> Input:
        """The connector object connected to this instance

        Returns ``None`` if no connection has been established
        """

        return self._partner

    def connect(self, connector: Input) -> None:
        """Establish the flow of data between this connector and another connector

        Args:
            connector: The connector object ot connect with
        """

        if type(connector) is type(self):
            raise ValueError('Cannot join together two connection objects of the same type.')

        if self.is_connected:
            raise exceptions.OverwriteConnectionError(
                'The current output connector is already connected to an input. Disconnect the output before re-connecting.')

        # Once a connection is established between two connectors, they share an internal queue
        self._partner = connector
        connector._connected_partners.add(self)
        self._queue = connector._queue

    def disconnect(self) -> None:
        """Disconnect any established connections"""

        if self.is_connected:
            self._partner._connected_partners.remove(self)
            self._partner = None
            self._queue = None

    def put(self, x: Any, raise_missing_connection: bool = True) -> None:
        """Add data into the connector

        Args:
            x: The value to put into the connector
            raise_missing_connection: Raise an error if trying to put data into an unconnected output

        Raises:
            MissingConnectionError: If trying to put data into an output that isn't connected to an input
        """

        if not self.is_connected and raise_missing_connection:
            raise MissingConnectionError('Output connector is not connected to an input to send data to.')

        self._queue.put(x)

    def __repr__(self) -> str:  # pragma: no cover
        return f'<egon.connectors.Output(name={self.name}) object at {hex(id(self))}>'
