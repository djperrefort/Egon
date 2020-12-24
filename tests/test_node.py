from functools import partial
from unittest import TestCase

from . import mock


class ProcessAllocation(TestCase):
    """Test ``Node`` instances fork the correct number of processes"""

    def runTest(self) -> None:
        num_processes = 4
        node = mock.MockNode(num_processes)
        self.assertEqual(num_processes, node.num_processes)
        self.assertEqual(num_processes, len(node._processes))


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

    def test_process_is_finished_on_execute(self) -> None:
        """Test the ``finished`` property is updated after node execution"""

        self.assertFalse(self.node.process_finished, 'Default finished state is not False.')
        self.node.execute()
        self.assertTrue(self.node.process_finished)


class TreeNavigation(TestCase):
    """Test ``Node`` instances are aware of their neighbors"""

    def setUp(self) -> None:
        """Create a tree of ``MockNode`` instances"""

        self.root = mock.MockSource()
        self.internal_node = mock.MockNode()
        self.leaf = mock.MockTarget()

        self.root.output.connect(self.internal_node.input)
        self.internal_node.output.connect(self.leaf.input)

    def test_upstream_nodes(self) -> None:
        """Test the inline node resolves the correct parent node"""

        self.assertEqual(self.root, self.internal_node.upstream_nodes()[0])

    def test_downstream_nodes(self) -> None:
        """Test the inline node resolves the correct child node"""

        self.assertEqual(self.leaf, self.internal_node.downstream_nodes()[0])
