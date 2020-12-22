from egon.connectors import Input, Output
from egon.nodes import Node, Source, Target


class MockNode(Node):
    """A ``Node`` subclass that implements placeholder functions for abstract methods"""

    def _validate_init(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""


class MockSource(Source, MockNode):
    """A ``Source`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self) -> None:
        self.output = Output()
        self.second_output = Output()


class MockTarget(Target, MockNode):
    """A ``Target`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self) -> None:
        self.input = Input()
        self.second_input = Input()


class MockInline(MockSource, MockTarget):
    """A ``Inline`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self) -> None:
        self.output = Output()
        self.second_output = Output()
        self.input = Input()
        self.second_input = Input()

    def _validate_init(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""
