"""Tests for the ``mock`` module."""

from unittest import TestCase

from egon.mock import MockSource, MockTarget


class MockNodeTroughput(TestCase):

    def runTest(self):
        test_data = [1, 2, 3]
        source = MockSource(test_data)
        target = MockTarget()
        source.output.connect(target.input)

        source.execute()
        target.execute()

        self.assertListEqual(test_data, target.accumulated_data)
