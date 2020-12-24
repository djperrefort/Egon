"""Tests for connector objects defined in the ``connectors`` module"""
from asyncio import sleep
from unittest import TestCase

from egon import exceptions
from egon.connectors import Connector, Input, Output
from tests.mock import MockSource, MockTarget


class InstanceConnections(TestCase):
    """Test the connection of generic connector objects to other"""

    def setUp(self) -> None:
        """Define a generic ``Connector`` instance"""

        self.connector = Connector()

    def test_overwrite_error_on_connection_overwrite(self) -> None:
        """An error is raised when trying to overwrite an existing connection"""

        self.connector.connect(Input())
        with self.assertRaises(exceptions.OverwriteConnectionError):
            self.connector.connect(Input())

    def test_overwrite_error_on_connected_argument(self) -> None:
        """An error is raised when trying to connect to a connector with an established connection"""

        input_connector = Input()
        output_connector = Output()
        input_connector.connect(output_connector)

        with self.assertRaises(exceptions.OverwriteConnectionError):
            self.connector.connect(input_connector)

    def test_connected_instances_share_queue(self) -> None:
        """Test two connected instances share the same memory queue"""

        input_connector = Input()
        output_connector = Output()
        input_connector.connect(output_connector)
        self.assertIs(input_connector._queue, output_connector._queue)

    def test_is_connected_boolean(self) -> None:
        """The ``is_connected`` method returns the current connection state"""

        input_connector = Input()
        self.assertFalse(input_connector.is_connected())
        input_connector.connect(Output())
        self.assertTrue(input_connector.is_connected())

    def test_error_on_connection_to_same_type(self) -> None:
        """An error is raised when connecting two inputs together"""

        with self.assertRaises(ValueError):
            Connector().connect(Connector())


class PartnerMapping(TestCase):
    """Test connectors with an established connection correctly map to neighboring connectors/nodes"""

    def setUp(self) -> None:
        """Create two connected pipeline elements"""

        self.target = MockTarget()
        self.input_connector = self.target.input
        self.output_connector = MockSource().output
        self.output_connector.connect(self.input_connector)

    def test_is_aware_of_partner(self) -> None:
        """Test connectors map to the correct partner connector"""

        self.assertIs(self.input_connector.partner, self.output_connector)

    def test_is_aware_of_parent(self) -> None:
        """Test connectors map to their partner"""

        self.assertIs(self.input_connector.parent_node, self.target)


class InstanceDisconnect(TestCase):
    """Test the disconnection of two connectors"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        self.input = Input()
        self.output = Output()
        self.input.connect(self.output)

    def test_queue_is_rewritten(self) -> None:
        """Test connectors revert to having individual queues"""

        original_queue = self.input._queue
        self.input.disconnect()
        self.assertIsNot(self.input._queue, original_queue)
        self.assertIsNot(self.output._queue, original_queue)
        self.assertIsNot(self.input._queue, self.output._queue)

    def test_both_connectors_are_disconnected(self) -> None:
        """Test calling disconnect from one connector results in both connectors being disconnected"""

        self.input.disconnect()
        self.assertFalse(self.input.is_connected())
        self.assertFalse(self.output.is_connected())

    def test_no_error_on_successive_disconnect(self) -> None:
        """Test no errors are raised when disconnecting an instance with no connection"""

        Input().disconnect()


class InputGet(TestCase):
    """Test data retrieval from ``Input`` instances"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        # Create a node with an input connector
        self.target = MockTarget()

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


class OutputSet(TestCase):
    """Test data storage in ``Output`` instances"""

    def setUp(self) -> None:
        """Define an ``Input`` instance"""

        # Create a node with an output connector
        self.source = MockSource()

    def test_stores_value_in_queue(self) -> None:
        """Test the ``put`` method retrieves data from the underlying queue"""

        test_val = 'test_val'
        self.source.output.put(test_val)
        self.assertEqual(self.source.output._queue.get(), test_val)
