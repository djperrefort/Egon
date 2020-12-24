from egon.connectors import Input, Output
from egon.nodes import AbstractNode, Source, Target


class Mock(AbstractNode):
    """A ``AbstractNode`` subclass that implements placeholder functions for abstract methods"""

    def _validate_init(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""

    def action(self) -> None:
        """Placeholder function to satisfy requirements of abstract parent"""


class MockSource(Source, Mock):
    """A ``Source`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self) -> None:
        self.output = Output()
        self.second_output = Output()


class MockTarget(Target, Mock):
    """A ``Target`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self) -> None:
        self.input = Input()
        self.second_input = Input()


class MockNode(MockSource, MockTarget):
    """A ``Node`` subclass that implements placeholder functions for abstract methods"""

    def __init__(self) -> None:
        self.output = Output()
        self.second_output = Output()
        self.input = Input()
        self.second_input = Input()
