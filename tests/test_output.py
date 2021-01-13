"""Tests the connectivity and functionality of ``Input`` connector objects."""

from time import sleep
from unittest import TestCase

from egon import exceptions
from egon.connectors import Input, Output
from egon.exceptions import MissingConnectionError
from egon.mock import MockSource, MockTarget


class InstanceConnections(TestCase):
    """Test the connection of generic connector objects to other"""

    def setUp(self) -> None:
        """Define a generic ``Connector`` instance"""

        self.input_connector = Input()
        self.output_connector = Output()

    def test_error_on_connection_to_same_type(self) -> None:
        """An error is raised when connecting two inputs together"""

        with self.assertRaises(ValueError):
            self.output_connector.connect(Output())

    def test_overwrite_error_on_connection_overwrite(self) -> None:
        """An error is raised when trying to overwrite an existing connection"""

        input = Input()
        self.output_connector.connect(input)
        with self.assertRaises(exceptions.OverwriteConnectionError):
            self.output_connector.connect(input)

    def test_is_connected_boolean(self) -> None:
        """The ``has_connections`` method returns the current connection state"""

        self.assertFalse(self.output_connector.is_connected)
        self.output_connector.connect(self.input_connector)
        self.assertTrue(self.output_connector.is_connected)

    def test_multiple_input_support(self):
        """Test output connectors support sending data to multiple input connectors"""

        # Create one node to output data and two to accept it
        test_data = [1, 2, 3]
        source = MockSource(test_data)
        target_a = MockTarget()
        target_b = MockTarget()

        # Connect two outputs to the same input
        source.output.connect(target_a.input)
        source.output.connect(target_b.input)
        source.execute()
        sleep(1)  # Give the queue a chance to update

        # Both inputs should have received the same data from the output
        target_a.execute()
        self.assertListEqual(test_data, target_a.accumulated_data)

        target_b.execute()
        self.assertListEqual(test_data, target_b.accumulated_data)


class PartnerMapping(TestCase):
    """Test connectors with an established connection correctly map to neighboring connectors/nodes"""

    def setUp(self) -> None:
        """Create two connected pipeline elements"""

        self.input1 = Input()
        self.input2 = Input()
        self.output = Output()

        self.output.connect(self.input1)
        self.output.connect(self.input2)

    def test_is_aware_of_partners(self) -> None:
        """Test connectors map to the correct partner connector"""

        input_connectors = [self.input1, self.input2]
        self.assertCountEqual(input_connectors, self.output.get_partners())


class InstanceDisconnect(TestCase):
    """Test the disconnection of two connectors"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        self.input = Input()
        self.output = Output()
        self.output.connect(self.input)

    def test_both_connectors_are_disconnected(self) -> None:
        """Test calling disconnect from one connector results in both connectors being disconnected"""

        self.output.disconnect(self.input)
        self.assertNotIn(self.output, self.input.get_partners())

    def test_error_if_not_connected(self):
        with self.assertRaises(MissingConnectionError):
            Output().disconnect(Input())

    def test_is_connected_boolean(self) -> None:
        """The ``has_connections`` method returns the current connection state"""

        self.assertTrue(self.output.is_connected)
        self.output.disconnect(self.input)
        self.assertFalse(self.output.is_connected)


class ConnectorPut(TestCase):
    """Test data storage in ``Output`` instances"""

    def setUp(self) -> None:
        """Define an ``Output`` instance"""

        # Create a node with an output connector
        self.source = MockSource()
        self.target = MockTarget()
        self.source.output.connect(self.target.input)

    def test_stores_value_in_queue(self) -> None:
        """Test the ``put`` method retrieves data from the underlying queue"""

        test_val = 'test_val'
        self.source.output.put(test_val)
        self.assertEqual(self.target.input._queue.get(), test_val)

    def test_error_if_unconnected(self) -> None:
        with self.assertRaises(MissingConnectionError):
            Output().put(5)

    @staticmethod
    def test_error_override() -> None:
        Output().put(5, raise_missing_connection=False)


class PartnerMapping(TestCase):
    """Test connectors with an established connection correctly map to neighboring connectors/nodes"""

    def setUp(self) -> None:
        """Create two connected pipeline elements"""

        self.source = MockSource()
        self.target = MockTarget()
        self.source.output.connect(self.target.input)

        self.output_connector = self.source.output
        self.input_connector = self.target.input

    def test_is_aware_of_partner(self) -> None:
        """Test connectors map to the correct partner connector"""

        self.assertIn(self.input_connector, self.output_connector.get_partners())

    def test_is_aware_of_parent(self) -> None:
        """Test connectors map to their partner"""

        self.assertIs(self.output_connector.parent_node, self.source)
