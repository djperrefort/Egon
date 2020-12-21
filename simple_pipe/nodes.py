from __future__ import annotations

import abc
import inspect
import multiprocessing as mp
from abc import ABC
from itertools import chain
from typing import List, Union

from . import connectors


class Node(abc.ABC):
    """Base class for constructing pipeline nodes"""

    def __init__(self) -> None:
        """Represents a single pipeline node"""

        self.num_processes = 1
        self.processes: List[mp.Process] = []
        self.finished = False
        self._validate()

        for connection in chain(self.input_connections(), self.output_connections()):
            connection._node = self

    @abc.abstractmethod
    def _validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        pass

    def _get_attrs(self, attr_type=None) -> List:
        """Return a list of instance attributes matching the given type

        Args:
            attr_type: The object type to search for

        Returns:
            A list of attributes of type ``attr_type``
        """

        return [getattr(self, a[0]) for a in inspect.getmembers(self, lambda a: isinstance(a, attr_type))]

    def is_connected(self) -> bool:
        """Returns True if all inputs/outputs are connected to other nodes"""

        return all(c.is_connected() for c in self._get_attrs(connectors.Connector))

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

    def input_nodes(self) -> List[Union[Source, Inline]]:
        """Returns a list of upstream pipeline nodes is_connected to the current _node"""

        return [c._node for c in self.input_connections()]

    def output_nodes(self) -> List[Union[Inline, Target]]:
        """Returns a list of downstream pipeline nodes is_connected to the current _node"""

        return [c._node for c in self.output_connections()]

    def setup(self) -> None:
        """Setup tasks called before running ``action``"""

        pass

    @abc.abstractmethod
    def action(self) -> None:
        """The analysis task performed by the parent pipeline process"""

        pass

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


class Source(Node, ABC):
    """A pipeline process that only has output streams"""

    def _validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if self.input_connections():
            raise ValueError('Source objects cannot have upstream components')

        if not self.output_connections():
            raise ValueError('Source node has no output connectors')


class Target(Node, ABC):
    """A pipeline process that only has input streams"""

    def _validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if not self.input_connections():
            raise ValueError('Target node has no input connectors')

        if self.output_connections():
            raise ValueError('Source objects cannot have upstream components')

    def expecting_inputs(self) -> bool:
        """Return True if the node is still expecting data from upstream"""

        return not (
                all(n.finished for n in self.input_nodes()) and
                all(c.empty() for c in self.input_connections())
        )


class Inline(Target, Source, ABC):
    """A pipeline process that can have any number of input or output streams"""

    def _validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if not (self.input_connections() or self.output_connections()):
            raise ValueError('Inline node has no associated connectorsZ')
