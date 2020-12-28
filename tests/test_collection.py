"""Tests for the ``Collection`` class."""

from unittest import TestCase

from egon.connectors import Collection


class Add(TestCase):

    def runTest(self) -> None:
        collection = Collection([1])
        self.assertIn(1, collection)


class Remove(TestCase):

    def runTest(self) -> None:
        collection = Collection([1])
        collection.remove(1)
        self.assertNotIn(1, collection)


class CastList(TestCase):

    def runTest(self) -> None:
        test_data = [1, 2, 3, 4]
        self.assertCountEqual(list(Collection(test_data)), test_data)
