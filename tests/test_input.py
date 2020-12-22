import queue
from unittest import TestCase

from egon.connectors import Input, Output


class Get(TestCase):
    """Test data retrieval from ``Input`` instances"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        self.input = Input()

    def test_returns_queue_value(self) -> None:
        """Test the ``get`` method retrieves data from the underlying queue"""

        test_val = 'test_val'
        self.input._queue.put(test_val)
        self.assertEqual(self.input.get(), test_val)

    def test_block_false_does_not_block(self) -> None:
        """Setting ``block`` to false should not block execution even for an empty queue"""

        with self.assertRaises(queue.Empty):
            self.input.get(block=False)


class InstanceConnections(TestCase):
    """Test the connection of ``Input`` objects to other connectors"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        self.input = Input()

    def test_error_on_connection_to_input(self) -> None:
        """An error is raised when connecting two inputs together"""

        with self.assertRaises(ValueError):
            self.input.connect(Input())

    def test_connected_output_is_accessible(self) -> None:
        """Test the connected output connector is accessible under the ``source_connector`` attr"""

        output = Output()
        self.input.connect(output)
        self.assertEqual(self.input.source_connector, output)
