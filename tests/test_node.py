"""Tests for the class based construction of pipeline nodes."""

from functools import partial
from unittest import TestCase

from egon import mock


class ProcessAllocation(TestCase):
    """Test ``Node`` instances fork the correct number of processes"""

    def test_allocation_at_init(self) -> None:
        """Test the correct number of processes are allocated at init"""

        num_processes = 4
        node = mock.MockNode(num_processes)
        self.assertEqual(num_processes, node.num_processes)
        self.assertEqual(num_processes, len(node._processes))

    def test_reallocation(self) -> None:
        """Test a new set of processes are allocated when the number of processes is changed"""

        num_processes = 4
        node = mock.MockNode(num_processes=1)
        node.num_processes = 4
        self.assertEqual(num_processes, node.num_processes)

    def test_error_if_processes_are_alive(self) -> None:
        """Test a RuntimeError is raised when trying to reallocate processes on a running node"""

        node = mock.MockNode(num_processes=1)
        node._processes[0].start()
        with self.assertRaises(RuntimeError):
            node.num_processes = 1

    def test_error_on_negative_processes(self) -> None:
        """Assert a value error is raised when the ``num_processes`` attribute is set to a negative"""

        node = mock.MockNode(1)
        with self.assertRaises(ValueError):
            node.num_processes = -1

    def test_error_on_negative_processes_at_init(self) -> None:
        """Assert a value error is raised when a node is instantiated with a negative number"""

        with self.assertRaises(ValueError):
            mock.MockNode(-1)


class Execution(TestCase):
    """Test the execution of tasks assigned to a Node instance"""

    def setUp(self) -> None:
        """Create a testing node that tracks the execution method of it's methods"""

        self.node = mock.MockNode(num_processes=1)

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
        """Test the ``process_finished`` property is updated after node execution"""

        self.assertFalse(self.node.process_finished, 'Default finished state is not False.')
        self.node.execute()
        self.assertTrue(self.node.process_finished)

    def test_node_is_finished_on_execute(self) -> None:
        """Test the ``node_finished`` property is updated after node execution"""

        self.assertFalse(self.node.node_finished, 'Default finished state is not False.')
        self.node._processes[0].start()
        self.node._processes[0].join()
        self.assertTrue(self.node.node_finished)


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


class ExpectingData(TestCase):
    """Tests for the ``expecting_data`` function

    The ``expecting_data`` function combines two booleans.
    This class evaluates all four squares of the corresponding truth table
    """

    def setUp(self) -> None:
        """Create a tree of ``MockNode`` instances"""

        # Fork zero processes so we can control the node finished state as the state of the daemon process
        self.root = mock.MockSource(num_processes=0)
        self.node = mock.MockNode(num_processes=0)
        self.root.output.connect(self.node.input)

    def test_false_for_empty_queue_and_finished_parent(self) -> None:
        """Test the return is False for a EMPTY queue and a FINISHED PARENT node"""

        self.root.process_finished = True
        self.assertFalse(self.node.expecting_data())

    def test_true_if_input_queue_has_data(self) -> None:
        """Test the return is True for a NOT EMPTY queue and a FINISHED PARENT node"""

        self.root.process_finished = True
        self.node.input._queue.put(5)
        self.assertTrue(self.node.expecting_data())

    def test_true_if_parent_is_running(self) -> None:
        """Test the return is True for a EMPTY queue and a NOT FINISHED PARENT node"""

        self.root.process_finished = False
        self.assertTrue(self.node.expecting_data())

    def test_true_if_input_queue_has_data_and_parent_is_running(self) -> None:
        """Test the return is True for a NOT EMPTY queue and a NOT FINISHED PARENT node"""

        self.root.process_finished = False
        self.node.input._queue.put(5)
        self.assertTrue(self.node.expecting_data())
