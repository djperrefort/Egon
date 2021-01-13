"""Tests for the ``ObjectCollection`` class."""

from unittest import TestCase

from egon._utils import ObjectCollection


class Add(TestCase):
    """Test the addition of data to the collection"""

    def test_values_are_added(self) -> None:
        """Test the ``add`` method adds values to the collection"""

        collection = ObjectCollection()
        collection.add(1)
        self.assertIn(1, collection)

    def test_no_duplicate_entries(self) -> None:
        """Test the collection does not store duplicate entries"""

        collection = ObjectCollection()
        collection.add(1)
        collection.add(1)
        self.assertEqual(1, len(collection))


class Remove(TestCase):
    """Test the removal of data from the collection"""

    def runTest(self) -> None:
        collection = ObjectCollection()
        collection.add(1)
        collection.remove(1)
        self.assertNotIn(1, collection)


class CastList(TestCase):
    """Test the casting of a collection to a list"""

    def runTest(self) -> None:
        test_data = [1, 2, 3, 4]
        self.assertListEqual(list(ObjectCollection(test_data)), test_data)
