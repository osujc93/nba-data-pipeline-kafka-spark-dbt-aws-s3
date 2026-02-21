import time
import logging
import cProfile


class StatsLogger:
    """
    Logs CPU profiling statistics periodically in a separate thread.
    """

    def __init__(
        self,
        profiler: cProfile.Profile,
        logger: logging.Logger
    ) -> None:
        """
        Initialize the StatsLogger with a profiler and a logger.

        :param profiler: The cProfile profiler instance.
        :param logger: Logger for logging messages and profiling info.
        """
        self.profiler = profiler
        self.logger = logger

    def log_stats_periodically(self, interval: int = 120) -> None:
        """
        Log CPU profiling statistics every 'interval' seconds in a separate thread.

        :param interval: Number of seconds between logs.
        """
        import io
        import pstats

        while True:
            time.sleep(interval)
            s = io.StringIO()
            ps = pstats.Stats(self.profiler, stream=s).sort_stats("cumulative")
            self.logger.info(
                "[Periodic CPU profiling stats - Last %s seconds]:\n%s",
                interval,
                s.getvalue(),
            )
