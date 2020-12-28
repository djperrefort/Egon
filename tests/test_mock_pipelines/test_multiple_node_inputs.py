"""A pipeline where two source nodes feed into the same target node."""

from multiprocessing import Queue
from unittest import TestCase

from egon.decorators import as_source, as_target
from egon.pipeline import Pipeline

TEST_INPUTS = list(range(20))  # Input values for the pipeline
GLOBAL_QUEUE = Queue()  # For storing pipeline outputs


@as_source
def source_a() -> None:
    """Load data into the pipeline"""

    for i in TEST_INPUTS[:10]:
        yield i


@as_source
def source_b() -> None:
    """Load data into the pipeline"""

    for i in TEST_INPUTS[10:]:
        yield i


@as_target
def receiving_node(x) -> None:
    """Retrieve data out of the pipeline"""

    GLOBAL_QUEUE.put(x)


class Pipe(Pipeline):
    """A pipeline for generating and then adding numbers"""

    def __init__(self) -> None:
        self.source_a = source_a
        self.source_b = source_b
        self.receive_node = receiving_node

        self.source_a.output.connect(self.receive_node.input)
        self.source_b.output.connect(self.receive_node.input)
        self.validate()


class TestPipelineThroughput(TestCase):
    """Test all data makes it through the pipeline"""

    def runTest(self) -> None:
        """Compare input and output pipeline values"""

        # Run should populate the global queue
        Pipe().run()

        # Convert the queue into a list
        l = []
        while GLOBAL_QUEUE.qsize() != 0:
            l.append(GLOBAL_QUEUE.get())

        self.assertCountEqual(TEST_INPUTS, l)
