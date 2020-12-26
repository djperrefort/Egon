"""Tests for the ``Pipeline`` class"""

from time import sleep
from unittest import TestCase

from egon.connectors import Output
from egon.exceptions import OrphanedNodeError, MissingConnectionError
from egon.pipeline import Pipeline
from tests.mock import MockSource, MockNode, MockPipeline


class StartStopCommands(TestCase):
    """Test processes are launched and terminated on command"""

    def runTest(self) -> None:
        """Launch and then kill the process manager"""

        pipeline = MockPipeline()
        self.assertFalse(pipeline.any_alive())

        pipeline.run_async()
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
        self.assertEqual(expected_count, pipeline.num_processes())


class PipelineValidation(TestCase):
    """Test appropriate errors are raised for an invalid pipeline."""

    def test_orphaned_node(self) -> None:
        """Test a ``OrphanedNodeError`` for an unreachable node"""

        class Pipe(Pipeline):

            def __init__(self) -> None:
                self.node = MockNode()

        with self.assertRaises(OrphanedNodeError):
            Pipe().validate()

    def test_missing_connection(self) -> None:
        """Test a ``MissingConnectionError`` for an unconnected connector"""

        class Pipe(Pipeline):
            def __init__(self) -> None:
                self.root = MockSource()
                self.node = MockNode()
                self.node.second_output = Output()
                self.root.output.connect(self.node.input)

        with self.assertRaises(MissingConnectionError):
            Pipe().validate()


class NodeDiscovery(TestCase):
    """Test the pipeline is aware of all of its nodes"""

    def runTest(self) -> None:
        pipeline = MockPipeline()
        expected_nodes = [pipeline.root, pipeline.leaf]
        recovered_nodes = pipeline.get_nodes()
        self.assertSequenceEqual(set(expected_nodes), set(recovered_nodes))
