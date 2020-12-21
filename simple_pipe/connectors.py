from __future__ import annotations

import multiprocessing as mp
from dataclasses import dataclass


@dataclass
class AbstractConnector:
    """Base class for all connection objects"""

    _queue: mp.Queue

    def connected(self) -> bool:
        """Return whether the connector has been connected to a task"""

        return self._queue is None

    def connect(self, connector) -> None:
        """Pipe data to another connector"""

        connector._queue = self._queue


class Input(AbstractConnector):
    """Represents an input stream of data to a pipeline process"""

    def connect(self, connector: Output) -> None:
        """Connect the input instance to an output instance from an upstream process"""

        super(Input, self).connect(connector)


class Output(AbstractConnector):
    """Represents an output stream of data from a pipeline process"""

    def connect(self, connector: Input) -> None:
        """Connect the output instance to an input instance of a downstream process"""

        super(Output, self).connect(connector)
