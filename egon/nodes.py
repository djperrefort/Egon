"""The ``nodes`` module supports the construction of individual pipeline nodes.
``Source``, ``Node``,  and ``Target`` classes are provided for creating nodes
that produce, analyze, and costume data respectively.
"""

from __future__ import annotations

import abc
import inspect
import multiprocessing as mp
from abc import ABC
from typing import Collection, List, Optional, Union

from . import connectors, exceptions


def _get_nodes_from_connectors(connector_list: Collection[connectors.AbstractConnector]) -> List:
    """Return the parent nodes from a list of ``Connector`` objects
    
    Args:
        connector_list: The connectors to get parents of
        
    Returns:
        A list of node instances
    """

    nodes = []
    for c in connector_list:
        nodes.extend(p.parent_node for p in c.get_partners())

    return nodes


class AbstractNode(abc.ABC):
    """Base class for constructing pipeline nodes"""

    def __init__(self, num_processes=1) -> None:
        """Represents a single pipeline node"""

        if num_processes < 0:
            raise ValueError(f'Cannot instantiate a negative number of processes (got {num_processes}).')

        self._num_processes = num_processes
        self._pool: Optional[mp.Pool] = None

        # Make any connector attributes aware of their parent node
        for connection in self.get_connectors():
            connection._node = self

        # Track the state of the node
        self._is_running = False  # Whether any forked processes are running
        self._current_process_finished = False  # State of the current (forked) process
        self._node_finished = False  # State of all combined processes

    @property
    def num_processes(self) -> int:
        """The number of processes assigned to the current node"""

        return self._num_processes

    @num_processes.setter
    def num_processes(self, num_processes) -> None:
        """The number of processes assigned to the current node"""

        if num_processes < 0:
            raise ValueError(f'Cannot instantiate a negative number of forked processes (got {num_processes}).')

        if self._is_running:
            raise RuntimeError('Cannot change number of processes while node is running.')

        if self.num_processes == num_processes:  # pragma: no cover
            return

        self._num_processes = num_processes

    def get_connectors(self) -> List[connectors.AbstractConnector]:
        """Return any connector attributes associated with the node instance"""

        return self._get_attrs(connectors.AbstractConnector)

    @property
    def process_finished(self) -> bool:
        """Whether the current process has finished processing data"""

        return self._current_process_finished

    @property
    def node_finished(self) -> bool:
        """Whether all node processes have finished processing data"""

        return self._node_finished

    @abc.abstractmethod
    def validate(self) -> None:
        """Raise an exception if the node object was constructed improperly

        Raises:
            ValueError: For an invalid instance construction
        """

    def _validate_connections(self) -> None:
        """Raise an exception if any of the node's Inputs/Outputs are missing connections

        Raises:
            MissingConnectionError: For an invalid instance construction
        """

        for conn in self.get_connectors():
            if not conn.is_connected:
                raise exceptions.MissingConnectionError(
                    f'Connector {conn} does not have an established connection (Node: {conn.parent_node})')

    def _get_attrs(self, attr_type=None) -> List:
        """Return a list of instance attributes matching the given type

        Args:
            attr_type: The object type to search for

        Returns:
            A list of attributes of type ``attr_type``
        """

        return [getattr(self, a[0]) for a in inspect.getmembers(self, lambda a: isinstance(a, attr_type))]

    def upstream_nodes(self) -> List[Union[Source, Node]]:
        """Returns a list of nodes that are upstream from the current node"""

        return _get_nodes_from_connectors(self._get_attrs(connectors.Input))

    def downstream_nodes(self) -> List[Union[Node, Target]]:
        """Returns a list of nodes that are downstream from the current node"""

        return _get_nodes_from_connectors(self._get_attrs(connectors.Output))

    def setup(self) -> None:
        """Setup tasks called before running ``action``"""

    @abc.abstractmethod
    def action(self) -> None:
        """The primary analysis task performed by this node"""

    def teardown(self) -> None:
        """Teardown tasks called after running ``action``"""

    def execute(self) -> None:
        """Execute all pipeline node tasks in order

        Execution includes all ``setup``, ``action``, and ``teardown`` tasks.
        """

        self.setup()
        self.action()
        self.teardown()
        self._current_process_finished = True

    def _run_pool(self) -> None:
        """Launch a pool of processes targeting the ``execute`` method"""

        if self._is_running:
            raise RuntimeError('This node instance is already running.')

        if self._node_finished:
            raise RuntimeError('This node instance has already finished executing.')

        self._is_running = True
        self._pool = mp.Pool(self.num_processes)
        self._pool.apply(self.execute)
        self._is_running = False
        self._node_finished = True

    def expecting_data(self) -> bool:
        """Return whether the node is still expecting data from upstream"""

        for input_connector in self._get_attrs(connectors.Input):
            # IMPORTANT: The order of the following code blocks is crucial
            # We check for any running upstream nodes first
            for output_connector in input_connector.get_partners():
                if not output_connector.parent_node.node_finished:
                    return True

            # Check for any unprocessed data once we know there are no
            # nodes still populating any input queues
            if not input_connector.empty():
                return True

        return False

    def __str__(self) -> str:  # pragma: no cover
        return f'<{self.__repr__()} object at {hex(id(self))}>'

    def __repr__(self) -> str:  # pragma: no cover
        return f'{self.__class__.__name__}(num_processes={self.num_processes})'

    def __del__(self):
        if self._is_running:
            raise RuntimeError(f'Cannot delete a node while it is running (del called on node {self})')


class Source(AbstractNode, ABC):
    """A pipeline process that only has output streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            MalformedSourceError: For an invalid instance construction
            OrphanedNodeError: For an instance that is inaccessible by connectors
        """

        if self._get_attrs(connectors.Input):
            raise exceptions.MalformedSourceError('Source objects cannot have upstream components.')

        if not self._get_attrs(connectors.Output):
            raise exceptions.OrphanedNodeError('Source has no output connectors and is inaccessible by the pipeline.')

        self._validate_connections()


class Target(AbstractNode, ABC):
    """A pipeline process that only has input streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            MalformedTargetError: For an invalid instance construction
            OrphanedNodeError: For an instance that is inaccessible by connectors
        """

        if self._get_attrs(connectors.Output):
            raise exceptions.MalformedTargetError('Source objects cannot have upstream components.')

        if not self._get_attrs(connectors.Input):
            raise exceptions.OrphanedNodeError('Target has no input connectors and is inaccessible by the pipeline.')

        self._validate_connections()


class Node(Target, Source, ABC):
    """A pipeline process that can have any number of input or output streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            OrphanedNodeError: For an instance that is inaccessible by connectors
        """

        if not self.get_connectors():
            raise exceptions.OrphanedNodeError('Node has no associated connectors and is inaccessible by the pipeline.')

        self._validate_connections()
