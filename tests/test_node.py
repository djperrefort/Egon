from functools import partial
from unittest import TestCase

from . import mock


class Execution(TestCase):
    """Test the execution of tasks assigned to a Node instance"""

    def setUp(self) -> None:
        """Create a testing node that tracks the execution method of it's methods"""

        self.node = mock.MockNode()

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

        self.assertFalse(self.node.process_finished)
        self.node.execute()
        self.assertTrue(self.node.process_finished)


class TestConnectorLists(TestCase):
    """Test ``Node`` instances are aware of their connectors"""

    def setUp(self) -> None:
        """Create a ``MockNode`` instance"""

        self.node = mock.MockNode()

    def test_all_inputs_are_listed(self) -> None:
        """Test all input connectors are included in ``node.get_inputs``"""

        node_inputs = [self.node.input, self.node.second_input]
        self.assertListEqual(self.node.get_inputs(), node_inputs)

    def test_all_outputs_are_listed(self) -> None:
        """Test all output connectors are included in ``node._get_outputs``"""

        node_outputs = [self.node.output, self.node.second_output]
        self.assertListEqual(self.node._get_outputs(), node_outputs)


class TreeNavigation(TestCase):
    """Test ``Node`` instances are aware of their neighbors"""

    def setUp(self) -> None:
        """Create a tree of ``MockNode`` instances"""

        self.root_node = mock.MockSource()
        self.internal_node = mock.MockNode()
        self.leaf_node = mock.MockTarget()

        self.root_node.output.connect(self.internal_node.input)
        self.internal_node.output.connect(self.leaf_node.input)

    def test_parent_node(self) -> None:
        """Test the inline node resolves the correct parent node"""

        self.assertEqual(self.root_node, self.internal_node.upstream_nodes()[0])

    def test_child_node(self) -> None:
        """Test the inline node resolves the correct child node"""

        self.assertEqual(self.leaf_node, self.internal_node.downstream_nodes()[0])
