"""Build a simple two node pipeline and test all input data makes it through to the end"""

from multiprocessing import Queue
from unittest import TestCase

from egon.decorators import as_source, as_target
from egon.pipeline import Pipeline

test_vals = list(range(10))  # Input values for the pipeline
queue = Queue()  # For storing pipeline outputs


@as_source
def sending_node() -> None:
    """Load data into the pipeline"""

    for i in test_vals:
        yield i


@as_target
def receiving_node(x) -> None:
    """Retrieve data out of the pipeline"""

    queue.put(x)


class AddingPipeline(Pipeline):
    """A pipeline for generating and then adding numbers"""

    def __init__(self) -> None:
        self.send_node = sending_node
        self.receive_node = receiving_node
        self.send_node.output.connect(self.receive_node.input)
        self.validate()


class TestPipelineThroughput(TestCase):
    """Test all data makes it through the pipeline"""

    def runTest(self) -> None:
        """Compare input and ouput pipeline values"""

        # Run should populate the global queue
        AddingPipeline().run()

        # Convert the queue into a list
        l = []
        while queue.qsize() != 0:
            l.append(queue.get())

        self.assertListEqual(test_vals, l)
