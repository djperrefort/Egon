"""Tests the connectivity and functionality of ``Input`` connector objects."""
from asyncio import sleep
from unittest import TestCase

from egon.connectors import KillSignal, Input, Output
from egon.mock import MockSource, MockTarget


class InstanceConnections(TestCase):
    """Test the connection of generic connector objects to other"""

    def setUp(self) -> None:
        """Define a generic ``Connector`` instance"""

        self.input_connector = Input()
        self.output_connector = Output()

    def test_is_connected_boolean(self) -> None:
        """The ``has_connections`` method returns the current connection state"""

        self.assertFalse(self.input_connector.is_connected)
        self.output_connector.connect(self.input_connector)
        self.assertTrue(self.input_connector.is_connected)

    def test_queue_size_change(self) -> None:
        """Test connected outputs point at the correct input queue after changing the queue size"""

        input = Input(maxsize=10)
        output = Output()
        output.connect(input)

        input.maxsize = 5
        self.assertIs(input._queue, output._queue)


class PartnerMapping(TestCase):
    """Test connectors with an established connection correctly map to neighboring connectors/nodes"""

    def setUp(self) -> None:
        """Create two connected pipeline elements"""

        self.source1 = MockSource()
        self.source2 = MockSource()
        self.target = MockTarget()

        self.source1.output.connect(self.target.input)
        self.source2.output.connect(self.target.input)

    def test_is_aware_of_partners(self) -> None:
        """Test connectors map to the correct partner connector"""

        output_connectors = [self.source1.output, self.source2.output]
        self.assertCountEqual(output_connectors, self.target.input.get_partners())

    def test_is_aware_of_parent(self) -> None:
        """Test connectors map to their partner"""

        self.assertIs(self.target.input.parent_node, self.target)


class InputGet(TestCase):
    """Test data retrieval from ``Input`` instances"""

    def setUp(self) -> None:
        """Define a node with an attached ``Input`` instance"""

        self.target = MockTarget(num_processes=0)  # Run node in current process only

    def test_error_on_non_positive_refresh(self) -> None:
        """Test a ValueError is raised when ``refresh_interval`` is not a positive number"""

        with self.assertRaises(ValueError):
            self.target.input.get(timeout=15, refresh_interval=0)

        with self.assertRaises(ValueError):
            self.target.input.get(timeout=15, refresh_interval=-1)

    def test_returns_queue_value(self) -> None:
        """Test the ``get`` method retrieves data from the underlying queue"""

        test_val = 'test_val'
        self.target.input._queue.put(test_val)
        self.assertEqual(self.target.input.get(timeout=1000), test_val)

    def test_kill_signal_on_finished_parent_node(self) -> None:
        """Test a kill signal is returned if the parent node if finished"""

        source = MockSource(num_processes=0)
        source.output.connect(self.target.input)
        source.process_finished = True
        self.assertFalse(self.target.expecting_data())
        self.assertIs(self.target.input.get(timeout=15), KillSignal)

    def test_timeout_raises_timeout_error(self) -> None:
        """Test a ``TimeoutError`` is raise on timeout"""

        with self.assertRaises(TimeoutError):
            self.target.input.get(timeout=1)


class InputIterGet(TestCase):
    """Test iteration behavior of the ``iter_get`` method"""

    def setUp(self) -> None:
        """Define a node with an attached ``Input`` instance"""

        # Create a node with an input connector
        self.target = MockTarget()

    def test_raises_stop_iteration_on_kill_signal(self) -> None:
        """Test the iterator exits once it reaches a KillSignal object"""

        self.target.input._queue.put(KillSignal)
        with self.assertRaises(StopIteration):
            next(self.target.input.iter_get())

    def test_returns_queue_value(self) -> None:
        """Test the ``get`` method retrieves data from the underlying queue"""

        test_val = 'test_val'
        self.target.input._queue.put(test_val)
        self.assertEqual(next(self.target.input.iter_get()), test_val)


class MaxQueueSize(TestCase):
    """Tests the setting/getting of the maximum size for the underlying queue"""

    def setUp(self) -> None:
        self.connector = Input(maxsize=10)

    def test_maxsize(self) -> None:
        """Test the max queue size is set at __init__"""

        self.assertEqual(self.connector._queue._maxsize, self.connector.maxsize)

    def test_set_at_init(self) -> None:
        """Test the max queue size is set at __init__"""

        self.assertEqual(10, self.connector.maxsize)

    def test_changed_via_setter(self) -> None:
        """Test the size of the underlying queue is changed when setting the ``maxsize`` attribute"""

        self.connector.maxsize = 5
        self.assertEqual(5, self.connector.maxsize)

    def test_error_on_nonempty_queue(self) -> None:
        """Test a ``RuntimeError`` is raised when changing the size of a nonempty connector"""

        self.connector._queue.put(1)
        sleep(1)  # Let the queue update

        with self.assertRaises(RuntimeError):
            self.connector.maxsize += 1
