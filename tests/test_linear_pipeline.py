"""Build a simple two node pipeline and test all input data makes it
through to the end.
"""

from multiprocessing import Queue
from unittest import TestCase

from egon.decorators import as_node, as_source, as_target
from egon.pipeline import Pipeline

TESTING_VALS = list(range(10))  # Input values for the pipeline
GLOBAL_QUEUE = Queue()  # For storing pipeline outputs


@as_source
def sending_node() -> float:
    """Load data into the pipeline"""

    for i in TESTING_VALS:
        yield i


@as_node
def internal_node(x) -> float:
    return x


@as_target
def receiving_node(x) -> None:
    """Retrieve data out of the pipeline"""

    GLOBAL_QUEUE.put(x)


class AddingPipeline(Pipeline):
    """A pipeline for generating and then adding numbers"""

    def __init__(self) -> None:
        self.send_node = sending_node
        self.internal_node = internal_node
        self.receive_node = receiving_node

        self.send_node.output.connect(self.internal_node.input)
        self.internal_node.output.connect(self.receive_node.input)
        self.validate()


class TestPipelineThroughput(TestCase):
    """Test all data makes it through the pipeline"""

    def runTest(self) -> None:
        """Compare input and output pipeline values"""

        # Run should populate the global queue
        AddingPipeline().run()

        # Convert the queue into a list
        l = []
        while GLOBAL_QUEUE.qsize() != 0:
            l.append(GLOBAL_QUEUE.get())

        self.assertCountEqual(TESTING_VALS, l)
