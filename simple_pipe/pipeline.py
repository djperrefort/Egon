class KillSignal:
    """Signal used to indicate a forked process should try and exit gracefully"""

    pass


class Pipeline:
    """Handles the starting and termination of forked processes"""

    def kill(self) -> None:
        """Kill all running pipeline processes without trying to exit gracefully"""

        for p in self._processes:
            p.terminate()

    def wait_for_exit(self) -> None:
        """Wait for the pipeline to finish running before continuing execution"""

        for p in self._processes:
            p.join()

    def run(self) -> None:
        """Similar to ``run_async`` but blocks further execution until finished"""

        self.run_async()
        self.wait_for_exit()

    def run_async(self) -> None:
        """Start all processes asynchronously"""

        for p in self._processes:
            p.start()
