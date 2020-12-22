from functools import partial
from unittest import TestCase

from egon.connectors import Input, Output
from egon.nodes import Node


class TestingNode(Node):
    """A testing node that implements placeholder functions for abstract methods"""

    input = Input()
    output = Output()

    def _validate_init(self) -> None:
        pass

    def action(self) -> None:
        pass


class Execution(TestCase):
    """Test the execution of tasks assigned to a Node instance"""

    def setUp(self) -> None:
        """Create a testing node that tracks the execution method of it's methods"""

        self.node = TestingNode()

        # Track the call order of node functions
        self.call_list = []
        self.node.setup = partial(self.call_list.append, 'setup')
        self.node.action = partial(self.call_list.append, 'action')
        self.node.teardown = partial(self.call_list.append, 'teardown')

    def test_call_order(self) -> None:
        """Test that setup and teardown actions are called in the correct order"""

        expected_order = ['setup', 'action', 'teardown']
        self.node.execute()
        self.assertListEqual(self.call_list, expected_order)

    def test_is_finished_on_execute(self) -> None:
        """Test the ``finished`` property is updated after node execution"""

        self.assertFalse(self.node.finished)
        self.node.execute()
        self.assertTrue(self.node.finished)



