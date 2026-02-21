#!/usr/bin/env python3
import os
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
from typing import List

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# The five producer scripts we want to run concurrently
PRODUCER_SCRIPTS: List[str] = [
    'NBA_traditional_team_stats.py',
    'NBA_traditional_player_stats.py',
    'NBA_advanced_team_stats.py',
    'NBA_advanced_player_stats.py',
    'NBA_regular_season_standings.py',
    'NBA_team_boxscores.py',
    'NBA_player_boxscores.py',
    'NBA_player_miscellaneous_stats.py',
    'NBA_team_miscellaneous_stats.py',
    'NBA_player_violations_stats.py',
    'NBA_team_violations_stats.py',
]


def start_script(script_path: str, semaphore: Semaphore) -> None:
    """
    Starts the given producer script as a subprocess, waits for it to complete,
    and then releases the semaphore.
    """
    if not os.path.isfile(script_path):
        logger.error(f"Script not found: {script_path}")
        semaphore.release()
        return

    command: str = f"python {script_path}"
    try:
        process = subprocess.Popen(command, shell=True)
        logger.info(f"Started script {script_path} (pid={process.pid})")
        process.wait()
        logger.info(f"Script {script_path} completed (exit code={process.returncode}).")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to run {script_path}: {e}")
    except Exception as e:
        logger.error(f"An error occurred while running {script_path}: {e}")
    finally:
        semaphore.release()


def main() -> None:
    """
    Runs all producer scripts concurrently, with a maximum concurrency
    of up to 11 scripts at once.
    """
    max_concurrent_scripts: int = 11
    semaphore = Semaphore(max_concurrent_scripts)

    with ThreadPoolExecutor(max_workers=max_concurrent_scripts) as executor:
        futures = []
        for script_name in PRODUCER_SCRIPTS:
            script_path: str = os.path.join('/app/kafka_producers', script_name)
            semaphore.acquire()
            futures.append(executor.submit(start_script, script_path, semaphore))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred in one of the threads: {e}")


if __name__ == '__main__':
    main()
