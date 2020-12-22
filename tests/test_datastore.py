from unittest import TestCase

from egon.connectors import DataStore


class QueueProperties(TestCase):

    def setUp(self) -> None:
        self.data_store = DataStore()

    def test_size_matches_queue_size(self) -> None:
        self.assertEqual(self.data_store.size(), self.data_store._queue.qsize())
        self.data_store._queue.put(1)
        self.assertEqual(self.data_store.size(), self.data_store._queue.qsize())

    def test_full_method_matches_queue_state(self) -> None:

        data_store = DataStore(maxsize=1)
        self.assertEqual(data_store.full(), False)
        self.data_store._queue.put(1)
        self.assertEqual(data_store.full(), True)

    def test_size_method_matches_queue_state(self) -> None:

        self.assertEqual(self.data_store.size(), 0)
        self.data_store._queue.put(1)
        self.assertEqual(self.data_store.size(), 1)
