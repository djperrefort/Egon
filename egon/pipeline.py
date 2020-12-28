"""The Pipeline module defines the ``Pipeline`` class which is responsible
for representing collections of interconnected analysis steps (``Node``
objects) as a  single, coherent analysis pipeline. ``Pipeline`` instances
are also responsible for starting/stopping forked processes and validating
that nodes are properly interconnected.
"""

from __future__ import annotations

from asyncio.subprocess import Process
from inspect import getmembers
from typing import List

from . import nodes
from .nodes import Node


class Pipeline:
    """Manages a collection of nodes as a single analysis pipeline"""

    def validate(self) -> None:
        """Set up the pipeline and check for any invalid node states"""

        # Make sure the nodes are in a runnable condition before we start spawning _processes
        for node in self.get_nodes():
            node.validate()

    def _get_processes(self) -> List[Process]:
        """Return a list of processes forked by pipeline nodes"""

        # Collect all of the processes assigned to each node
        processes = []
        for node in self.get_nodes():
            processes.extend(node._processes)

        return processes

    def get_nodes(self) -> List[Node]:
        """Return a list of all nodes in the pipeline"""

        return [getattr(self, a[0]) for a in getmembers(self, lambda a: isinstance(a, nodes.AbstractNode))]

    def num_processes(self) -> int:
        """The number of processes forked by to the pipeline"""

        return len(self._get_processes())

    def kill(self) -> None:
        """Kill all running pipeline processes without trying to exit gracefully"""

        for p in self._get_processes():
            p.terminate()

    def run(self) -> None:
        """Start all pipeline processes and block execution until all processes exit"""

        self.run_async()
        self.wait_for_exit()

    def wait_for_exit(self) -> None:
        """Wait for the pipeline to finish running before continuing execution"""

        for p in self._get_processes():
            p.join()

    def run_async(self) -> None:
        """Start all processes asynchronously"""

        for p in self._get_processes():
            p.start()
