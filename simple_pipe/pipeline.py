from __future__ import annotations

import inspect
import multiprocessing as mp
from typing import List

from . import nodes


class KillSignal:
    _instance = None

    def __new__(cls, *args, **kwargs) -> KillSignal:
        if not cls._instance:
            cls._instance = super(KillSignal, cls).__new__(cls, *args, **kwargs)

        return cls._instance


class AbstractPipeline:
    """Base class for pipeline construction"""

    def nodes(self) -> List[nodes.Node]:
        """Returns a list of all nodes in the pipeline

        Returns:
            A list of ``Node`` instances
        """

        predicate = lambda a: isinstance(a, nodes.Node)
        return [getattr(self, a[0]) for a in inspect.getmembers(self, predicate)]

    def validate(self) -> None:
        """Check that the pipeline has no unconnected streams"""

        raise NotImplemented

    def visualize(self) -> None:
        """Create a graphic visualization of the pipeline"""

        raise NotImplemented


class Pipeline(AbstractPipeline):
    """Handles the starting and termination of forked processes"""

    def __init__(self) -> None:
        self.processes = []
        self._allocate_processes()

    def _allocate_processes(self) -> None:
        """Instantiate forked processes for each pipeline _node"""

        for node in self.nodes():
            for i in range(node.num_processes):
                self.processes.append(mp.Process(target=node.execute))

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
