"""Tests for the ``Connector`` class"""

import time
from unittest import TestCase

from egon.connectors import AbstractConnector


class QueueProperties(TestCase):
    """Test  test the exposure of queue properties by the overlying ``Connector`` class"""

    def setUp(self) -> None:
        """Create a ``DataStore`` instance"""

        self.connector = AbstractConnector(maxsize=1)

    def test_size_matches_queue_size(self) -> None:
        """Test the ``size`` method returns the size of the queue`"""

        self.assertEqual(self.connector.size(), 0)
        self.connector._queue.put(1)
        self.assertEqual(self.connector.size(), 1)

    def test_full_state(self) -> None:
        """Test the ``full`` method returns the state of the queue"""

        self.assertFalse(self.connector.full())
        self.connector._queue.put(1)
        self.assertTrue(self.connector.full())

    def test_empty_state(self) -> None:
        """Test the ``empty`` method returns the state of the queue"""

        self.assertTrue(self.connector.empty())
        self.connector._queue.put(1)

        # The value of Queue.empty() updates asynchronously
        time.sleep(1)

        self.assertFalse(self.connector.empty())
