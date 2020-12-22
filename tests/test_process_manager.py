from multiprocessing import Process
from time import sleep
from unittest import TestCase

from egon.pipeline import ProcessManager


def sleep_function() -> None:
    """Sleep for ten seconds"""

    sleep(10)


class StartStopCommands(TestCase):
    """Test processes are launched and terminated on command"""

    def setUp(self) -> None:
        """Create a ``ProcessManager`` instance with a single dummy process"""

        self.process = Process(target=sleep_function)
        self.manager = ProcessManager()
        self.manager.processes = [self.process]

    def runTest(self) -> None:
        """Launch and then kill the process manager"""

        self.assertFalse(self.process.is_alive())

        self.manager.run_async()
        sleep(1)  # Give the process time to start
        self.assertTrue(self.process.is_alive())

        self.manager.kill()
        sleep(1)  # Give the process time to exit
        self.assertFalse(self.process.is_alive())


class NumProcesses(StartStopCommands):

    def runTest(self) -> None:
        """Test the counting of child processes"""

        self.assertEqual(self.manager.process_count, 1)
