from asyncio import sleep

from egon import nodes
from egon.connectors import Input, Output
from egon.pipeline import Pipeline


class MockSource(nodes.Source):
    """A ``Source`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=1) -> None:
        self.output = Output()
        super(MockSource, self).__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

        sleep(10)


class MockTarget(nodes.Target):
    """A ``Target`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=1) -> None:
        self.input = Input()
        super(MockTarget, self).__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

        sleep(100)


class MockNode(nodes.Node):
    """A ``Node`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=1) -> None:
        self.output = Output()
        self.input = Input()
        super(MockNode, self).__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

        sleep(10)


class MockPipeline(Pipeline):
    """A mock pipeline with a root and a leaf"""

    def __init__(self) -> None:
        self.root = MockSource(num_processes=2)
        self.leaf = MockTarget()
        self.root.output.connect(self.leaf.input)

        self.validate()

    def all_alive(self) -> bool:
        """Return if all processes managed by the pipeline are alive"""

        return all(p.is_alive() for p in self._get_processes())

    def any_alive(self) -> bool:
        """Return if any process managed by the pipeline are alive"""

        return any(p.is_alive() for p in self._get_processes())
