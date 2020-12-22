from __future__ import annotations

import inspect
import multiprocessing as mp
from typing import List

from . import nodes


class ProcessManager:
    """Handles the starting and termination of forked processes"""

    processes: List

    @property
    def process_count(self) -> int:
        """The number of processes assigned to the pipeline"""

        return len(self.processes)

    def kill(self) -> None:
        """Kill all running pipeline processes without trying to exit gracefully"""

        for p in self.processes:
            p.terminate()

    def run(self) -> None:
        """Start all pipeline processes and block execution until all processes exit"""

        self.run_async()
        self.wait_for_exit()

    def wait_for_exit(self) -> None:
        """Wait for the pipeline to finish running before continuing execution"""

        for p in self.processes:
            p.join()

    def run_async(self) -> None:
        """Start all processes asynchronously"""

        for p in self.processes:
            p.start()


class Pipeline(ProcessManager):
    """Manages a collection of nodes as a single analysis pipeline"""

    def setup_pipeline(self) -> None:
        """Set up the pipeline and instantiate child processes"""

        # Make sure the nodes are in a runnable condition before we start spawning processes
        self._validate_nodes()

        self.processes = []
        for node in self.nodes():
            for i in range(node.num_processes):
                self.processes.append(mp.Process(target=node.execute))

    def nodes(self) -> List[nodes.Node]:
        """Returns a list of all nodes in the pipeline

        Returns:
            A list of ``Node`` instances
        """

        return [getattr(self, a[0]) for a in inspect.getmembers(self, lambda a: isinstance(a, nodes.Node))]

    def _validate_nodes(self) -> None:
        """Check that the pipeline has no nodes with unassigned connectors"""

        if not all(n._validate_connections() for n in self.nodes()):
            raise RuntimeError('Pipeline cannot run with disconnected inputs/outputs')
