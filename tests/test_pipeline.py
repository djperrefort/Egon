"""Tests for the ``Pipeline`` class"""

from time import sleep
from unittest import TestCase

from egon.exceptions import OrphanedNodeError, MissingConnectionError
from egon.pipeline import Pipeline
from tests.mock import MockSource, MockTarget, MockNode


class MockPipeline(Pipeline):
    """A mock pipeline with a root and a leaf"""

    def __init__(self) -> None:
        self.root = MockSource(num_processes=2)
        self.leaf = MockTarget()
        self.root.output.connect(self.leaf.input)

        self.validate()

    def all_alive(self) -> bool:
        """Return if all processes managed by the pipeline are alive"""

        return all(p.all_alive() for p in self._processes)

    def any_alive(self) -> bool:
        """Return if any process managed by the pipeline are alive"""

        return any(p.all_alive() for p in self._processes)


class StartStopCommands(TestCase):
    """Test processes are launched and terminated on command"""

    def runTest(self) -> None:
        """Launch and then kill the process manager"""

        pipeline = MockPipeline()
        self.assertFalse(pipeline.any_alive())

        pipeline.run_async()
        sleep(1)  # Give the process time to start
        self.assertTrue(pipeline.all_alive())

        pipeline.kill()
        sleep(1)  # Give the process time to exit
        self.assertFalse(pipeline.any_alive())


class ProcessCount(TestCase):
    """Test the pipelines process count matches the sum of processes allocated to each node"""

    def runTest(self) -> None:
        """Launch and then kill the process manager"""

        pipeline = MockPipeline()
        expected_count = pipeline.root.num_processes + pipeline.leaf.num_processes
        self.assertEqual(expected_count, pipeline.num_processes)


class PipelineValidation(TestCase):
    """Test appropriate errors are raised for an invalid pipeline."""

    def test_orphaned_node(self) -> None:
        """Test a ``OrphanedNodeError`` for an unreachable node"""

        class Pipe(Pipeline):
            self.node = MockNode()

        with self.assertRaises(OrphanedNodeError):
            Pipe().validate()

    def test_missing_connection(self) -> None:
        """Test a ``MissingConnectionError`` for an unconnected connector"""

        class Pipe(Pipeline):
            self.root = MockSource()
            self.node = MockNode()
            self.root.output.connect(self.node.input)

        with self.assertRaises(MissingConnectionError):
            Pipe().validate()


class NodeDiscovery(TestCase):
    """Test the pipeline is aware of all of its nodes"""

    def runTest(self) -> None:
        pipeline = MockPipeline()
        expected_nodes = [pipeline.root, pipeline.leaf]
        self.assertListEqual(expected_nodes, pipeline.nodes)
