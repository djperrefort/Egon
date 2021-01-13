"""The ``nodes`` module supports the construction of individual pipeline nodes.
``Source``, ``Node``,  and ``Target`` classes are provided for creating nodes
that produce, analyze, and costume data respectively.
"""

from __future__ import annotations

import abc
import inspect
import multiprocessing as mp
import sys
from abc import ABC
from typing import Collection, List, Union

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
            raise ValueError(f'Cannot instantiate a negative number of forked processes (got {num_processes}).')

        # Note that we use the memory address and not the ``pid`` attribute.
        # ``pid`` is only set after the process is started
        self._processes = [mp.Process(target=self.execute) for _ in range(num_processes)]
        self._states = mp.Manager().dict({id(p): False for p in self._processes})

        self._current_process_state = False
        for connection in self._get_attrs(connectors.AbstractConnector):
            connection._node = self

    @property
    def num_processes(self) -> int:
        """The number of processes assigned to the current node"""

        return len(self._processes)

    @num_processes.setter
    def num_processes(self, num_processes) -> None:
        """The number of processes assigned to the current node"""

        if num_processes < 0:
            raise ValueError(f'Cannot instantiate a negative number of forked processes (got {num_processes}).')

        if any(p.is_alive() for p in self._processes):
            raise RuntimeError('Cannot change number of processes while node is running.')

        if self.num_processes == num_processes:  # pragma: no cover
            return

        self._processes = [mp.Process(target=self.execute) for _ in range(num_processes)]
        self._states = mp.Manager().dict({id(p): False for p in self._processes})

    @property
    def process_finished(self) -> bool:
        """Return whether the current process has finished processing data"""

        # Use get in case called from a process not forked by the class __init__
        return self._states.get(mp.current_process().pid, self._current_process_state)

    @process_finished.setter
    def process_finished(self, state: bool) -> None:
        self._states[id(mp.current_process())] = self._current_process_state = state

    @property
    def node_finished(self) -> bool:
        """Return whether all node processes have finished processing data"""

        return all(self._states.values())

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

        for conn in self._get_attrs(connectors.AbstractConnector):
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
        """Execute the pipeline node

        Execution includes all ``setup``, ``action``, and ``teardown`` tasks.
        """

        self.setup()
        self.action()
        self.teardown()
        self.process_finished = True

    def expecting_data(self) -> bool:
        """Return whether the node is still expecting data from upstream"""

        for input_connector in self._get_attrs(connectors.Input):
            # IMPORTANT: The order of the following code blocks is crucial
            # We check for any running upstream nodes first
            for partner in input_connector.get_partners():
                if not partner.parent_node.node_finished:
                    return True

            # Check for any unprocessed data once we know there are no
            # nodes still populating any input queues
            if not input_connector.empty():
                return True

        return False


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

        if not self._get_attrs(connectors.AbstractConnector):
            raise exceptions.OrphanedNodeError('Node has no associated connectors and is inaccessible by the pipeline.')

        self._validate_connections()
