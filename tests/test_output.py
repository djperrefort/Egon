"""Tests the connectivity and functionality of ``Input`` connector objects."""

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

        self.output_connector.connect(Input())
        with self.assertRaises(exceptions.OverwriteConnectionError):
            self.output_connector.connect(Input())

    def test_connected_instances_share_queue(self) -> None:
        """Test two connected instances share the same memory queue"""

        self.output_connector.connect(self.input_connector)
        self.assertIs(self.output_connector._queue, self.input_connector._queue)

    def test_is_connected_boolean(self) -> None:
        """The ``has_connections`` method returns the current connection state"""

        self.assertFalse(self.output_connector.is_connected)
        self.output_connector.connect(self.input_connector)
        self.assertTrue(self.output_connector.is_connected)


class InstanceDisconnect(TestCase):
    """Test the disconnection of two connectors"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        self.input = Input()
        self.output = Output()
        self.output.connect(self.input)

    def test_queue_deleted(self) -> None:
        """Test connectors revert to having individual queues"""

        self.output.disconnect()
        self.assertIsNone(self.output._queue)

    def test_both_connectors_are_disconnected(self) -> None:
        """Test calling disconnect from one connector results in both connectors being disconnected"""

        self.output.disconnect()
        self.assertNotIn(self.output, self.input._connected_partners)

    @staticmethod
    def test_no_error_on_successive_disconnect() -> None:
        """Test no errors are raised when disconnecting an instance with no connection"""

        Output().disconnect()

    def test_is_connected_boolean(self) -> None:
        """The ``has_connections`` method returns the current connection state"""

        self.assertTrue(self.output.is_connected)
        self.output.disconnect()
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
        self.assertEqual(self.source.output._queue.get(), test_val)

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

        self.assertIs(self.output_connector.get_partner(), self.input_connector)

    def test_is_aware_of_parent(self) -> None:
        """Test connectors map to their partner"""

        self.assertIs(self.output_connector.parent_node, self.source)
