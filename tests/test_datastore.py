"""Tests for the ``DataStore`` class"""

import time
from unittest import TestCase

from egon.connectors import DataStore


class QueueProperties(TestCase):
    """Test  test the exposure of queue properties by the overlying ``DataStore`` class"""

    def setUp(self) -> None:
        """Create a ``DataStore`` instance"""

        self.data_store = DataStore(maxsize=1)

    def test_size_matches_queue_size(self) -> None:
        """Test the ``size`` method returns the size of the queue`"""

        self.assertEqual(self.data_store.size(), 0)
        self.data_store._queue.put(1)
        self.assertEqual(self.data_store.size(), 1)

    def test_full_state(self) -> None:
        """Test the ``full`` method returns the state of the queue"""

        self.assertFalse(self.data_store.full())
        self.data_store._queue.put(1)
        self.assertTrue(self.data_store.full())

    def test_empty_state(self) -> None:
        """Test the ``empty`` method returns the state of the queue"""

        self.assertTrue(self.data_store.empty())
        self.data_store._queue.put(1)

        # The value of Queue.empty() updates asynchronously
        time.sleep(1)

        self.assertFalse(self.data_store.empty())
