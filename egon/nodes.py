from __future__ import annotations

import abc
import inspect
import multiprocessing as mp
from abc import ABC
from itertools import chain
from typing import List, Union

from . import connectors, exceptions


class AbstractNode(abc.ABC):
    """Base class for constructing pipeline nodes"""

    def __init__(self) -> None:
        """Represents a single pipeline node"""

        self.num_processes = 1
        self._finished = mp.Value('i', 0)

        for connection in chain(self.input_connections(), self.output_connections()):
            connection._node = self

        self._validate_init()

    @property
    def finished(self) -> bool:
        """Whether the pipeline has finished processing data"""

        return bool(self._finished.value)

    @finished.setter
    def finished(self, val: bool):
        """Cast boolean to integer and store as a ctype in memory"""

        self._finished.value = int(val)

    @abc.abstractmethod
    def _validate_init(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

    def _validate_connections(self) -> None:
        """Raise exception if any of the node's Inputs/Outputs are missing connections

        Raises:
            MissingConnectionError: For an invalid instance construction
        """

        for conn in self._get_attrs(connectors.Connector):
            if not conn.is_connected():
                raise exceptions.MissingConnectionError(
                    f'Connector {conn} does not have an established connection (Node: {conn.node})')

    def _get_attrs(self, attr_type=None) -> List:
        """Return a list of instance attributes matching the given type

        Args:
            attr_type: The object type to search for

        Returns:
            A list of attributes of type ``attr_type``
        """

        return [getattr(self, a[0]) for a in inspect.getmembers(self, lambda a: isinstance(a, attr_type))]

    def input_connections(self) -> List[connectors.Input]:
        """Returns a list of input connections assigned to the current _node

        Returns:
            A list of ``Input`` connection objects
        """
        return self._get_attrs(connectors.Input)

    def output_connections(self) -> List[connectors.Output]:
        """Returns a list of output connections assigned to the current _node

        Returns:
            A list of ``Output`` connection objects
        """

        return self._get_attrs(connectors.Output)

    def input_nodes(self) -> List[Union[Source, Node]]:
        """Returns a list of upstream pipeline nodes _validate_connections to the current _node"""

        return list(filter(None, (c.source_node for c in self.input_connections())))

    def output_nodes(self) -> List[Union[Node, Target]]:
        """Returns a list of downstream pipeline nodes _validate_connections to the current _node"""

        return list(filter(None, (c.destination_node for c in self.output_connections())))

    def setup(self) -> None:
        """Setup tasks called before running ``action``"""

    @abc.abstractmethod
    def action(self) -> None:
        """The analysis task performed by the parent pipeline process"""

    def teardown(self) -> None:
        """Teardown tasks called after running ``action``"""

    def execute(self) -> None:
        """Execute the pipeline node

        Execution includes all ``setup``, ``action``, and ``teardown`` tasks.
        Once execution is, the ``finished`` attribute is set to ``True``
        """

        self.setup()
        self.action()
        self.teardown()
        self.finished = True


class Source(AbstractNode, ABC):
    """A pipeline process that only has output streams"""

    def _validate_init(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            MalformedSourceError: For an invalid instance construction
            OrphanedNodeError: For an instance that is inaccessible by connectors
        """

        if self.input_connections():
            raise exceptions.MalformedSourceError('Source objects cannot have upstream components.')

        if not self.output_connections():
            raise exceptions.OrphanedNodeError('Source has no output connectors and is inaccessible by the pipeline.')


class Target(AbstractNode, ABC):
    """A pipeline process that only has input streams"""

    def _validate_init(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            MalformedTargetError: For an invalid instance construction
            OrphanedNodeError: For an instance that is inaccessible by connectors
        """

        if self.output_connections():
            raise exceptions.MalformedTargetError('Source objects cannot have upstream components.')

        if not self.input_connections():
            raise exceptions.OrphanedNodeError('Target has no input connectors and is inaccessible by the pipeline.')

    def expecting_inputs(self) -> bool:
        """Return True if the node is still expecting data from upstream"""

        return not (
                all(n.finished for n in self.input_nodes()) and
                all(c.empty() for c in self.input_connections())
        )


class Node(Target, Source, ABC):
    """A pipeline process that can have any number of input or output streams"""

    def _validate_init(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            OrphanedNodeError: For an instance that is inaccessible by connectors
        """

        if not (self.input_connections() or self.output_connections()):
            raise exceptions.OrphanedNodeError('Node has no associated connectors and is inaccessible by the pipeline.')
