from __future__ import annotations

from asyncio.subprocess import Process
from inspect import getmembers
from typing import List

from . import nodes
from .nodes import Node


class Pipeline:
    """Manages a collection of nodes as a single analysis pipeline"""

    def validate(self) -> None:
        """Set up the pipeline and instantiate child processes"""

        # Make sure the nodes are in a runnable condition before we start spawning _processes
        for node in self.get_nodes():
            node.validate_connections()

    def _get_processes(self) -> List[Process]:
        """Return a list of all processes forked by pipeline nodes"""

        # Collect all of the processes assigned to each node
        processes = []
        for node in self.get_nodes():
            processes.extend(node._processes)

        return processes

    def get_nodes(self) -> List[Node]:
        """Return a list of nodes in the pipeline"""

        return [getattr(self, a[0]) for a in getmembers(self, lambda a: isinstance(a, nodes.AbstractNode))]

    def num_processes(self) -> int:
        """The number of _processes assigned to the pipeline"""

        return len(self._get_processes())

    def kill(self) -> None:
        """Kill all running pipeline _processes without trying to exit gracefully"""

        for p in self._get_processes():
            p.terminate()

    def run(self) -> None:
        """Start all pipeline _processes and block execution until all processes exit"""

        self.run_async()
        self.wait_for_exit()

    def wait_for_exit(self) -> None:
        """Wait for the pipeline to finish running before continuing execution"""

        for p in self._get_processes():
            p.join()

    def run_async(self) -> None:
        """Start all _processes asynchronously"""

        for p in self._get_processes():
            p.start()
