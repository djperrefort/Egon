from unittest import TestCase

from egon.connectors import KillSignal
from tests.mock import MockTarget, MockSource


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
