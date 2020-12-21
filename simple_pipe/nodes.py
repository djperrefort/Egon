from __future__ import annotations

import abc
import inspect
import multiprocessing as mp
from abc import ABC
from itertools import chain
from typing import List, Union, cast

from . import connectors


class Node(abc.ABC):
    """Base class for constructing pipeline nodes"""

    def __init__(self) -> None:
        """Represents a single pipeline _node"""

        self.validate()

        for source in self.input_connections():
            source._node = self

        for target in self.output_connections():
            target._node = self

        self.num_processes = 1
        self.processes: List[mp.Process] = []

    @abc.abstractmethod
    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        pass

    def connected(self) -> bool:
        """Returns True if all inputs/outputs are connected to other nodes"""

        return all(c.connected() for c in chain(self.input_connections(), self.output_connections()))

    def input_connections(self) -> List[connectors.Input]:
        """Returns a list of input connections assigned to the current _node

        Returns:
            A list of ``Input`` connection objects
        """

        predicate = lambda a: isinstance(a, connectors.Input)
        return [getattr(self, a[0]) for a in inspect.getmembers(self, predicate)]

    def output_connections(self) -> List[connectors.Output]:
        """Returns a list of output connections assigned to the current _node

        Returns:
            A list of ``Output`` connection objects
        """

        predicate = lambda a: isinstance(a, connectors.Output)
        return [getattr(self, a[0]) for a in inspect.getmembers(self, predicate)]

    def input_nodes(self) -> Union[Source, Inline]:
        """Returns a list of upstream pipeline nodes connected to the current _node"""

        nodes = [c._node for c in self.input_connections()]
        return cast(Union[Source, Inline], nodes)

    def output_nodes(self) -> Union[Inline, Target]:
        """Returns a list of downstream pipeline nodes connected to the current _node"""

        nodes = [c._node for c in self.output_connections()]
        return cast(Union[Inline, Target], nodes)

    def setup(self) -> None:
        """Setup tasks called before running ``action``"""

        pass

    @abc.abstractmethod
    def action(self) -> None:
        """The analysis task performed by the parent pipeline process"""

        pass

    def teardown(self) -> None:
        """Teardown tasks called after running ``action``"""

        pass

    def run(self):
        self.setup()
        self.action()
        self.teardown()


class Source(Node, ABC):
    """A pipeline process that only has output streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if self.input_connections():
            raise ValueError('Source objects cannot have upstream components')


class Target(Node, ABC):
    """A pipeline process that only has input streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if self.output_connections():
            raise ValueError('Source objects cannot have upstream components')


class Inline(Source, Target, ABC):
    """A pipeline process that can have any number of input or output streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        pass
