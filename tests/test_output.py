from unittest import TestCase

from egon.connectors import Input, Output


class Set(TestCase):
    """Test data storage in ``Output`` instances"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        self.output = Output()

    def test_stores_value_in_queue(self) -> None:
        """Test the ``put`` method retrieves data from the underlying queue"""

        test_val = 'test_val'
        self.output.put(test_val)
        self.assertEqual(self.output._queue.get(), test_val)


class InstanceConnections(TestCase):
    """Test the connection of ``Output`` objects to other connectors"""

    def setUp(self) -> None:
        """Define an ``Output`` instance"""

        self.output = Output()

    def test_error_on_connection_to_input(self) -> None:
        """An error is raised when connecting two inputs together"""

        with self.assertRaises(ValueError):
            self.output.connect(Output())

    def test_connected_output_is_accessible(self) -> None:
        """Test the connected input connector is accessible under the ``destination_connector`` attr"""

        input = Input()
        self.output.connect(input)
        self.assertEqual(self.output.destination_connector, input)