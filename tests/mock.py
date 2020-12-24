from egon import nodes
from egon.connectors import Input, Output


class MockSource(nodes.Source):
    """A ``Source`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=1) -> None:
        self.output = Output()
        self.second_output = Output()
        super().__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""


class MockTarget(nodes.Target):
    """A ``Target`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=1) -> None:
        self.input = Input()
        self.second_input = Input()
        super().__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""


class MockNode(nodes.Node):
    """A ``Node`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self, num_processes=1) -> None:
        self.output = Output()
        self.second_output = Output()
        self.input = Input()
        self.second_input = Input()
        super().__init__(num_processes)

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""
