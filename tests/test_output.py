from unittest import TestCase

from tests.mock import MockSource


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
