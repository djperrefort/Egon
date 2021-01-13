"""The ``mock`` module defines prebuilt pipeline nodes for developing
unittests. Instead of accomplishing a user defined action, mock nodes sleep
for a pre-defined number of seconds.
"""

from egon import nodes
from egon.connectors import Input, Output
from egon.pipeline import Pipeline


class MockSource(nodes.Source):
    """A ``Source`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, load_data: list = None, num_processes=0) -> None:
        self.output = Output()
        self.load_data = load_data or []
        super(MockSource, self).__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

        for x in self.load_data:
            self.output.put(x)


class MockTarget(nodes.Target):
    """A ``Target`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=0) -> None:
        self.input = Input()
        self.accumulated_data = []
        super(MockTarget, self).__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

        for x in self.input.iter_get():
            self.accumulated_data.append(x)


class MockNode(nodes.Node):
    """A ``Node`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=0) -> None:
        self.output = Output()
        self.input = Input()
        super(MockNode, self).__init__(num_processes)

    def action(self) -> None:  # pragma: no cover
        """Placeholder function to satisfy requirements of abstract parent"""

        for x in self.input.iter_get():
            self.output.put(x)


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
