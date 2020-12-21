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
