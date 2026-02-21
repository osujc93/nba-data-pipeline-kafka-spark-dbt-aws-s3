#!/usr/bin/env python3
import os
import subprocess
import logging
from typing import List

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# The producer scripts we want to run, one at a time
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

def main() -> None:
    """
    Runs all producer scripts in a fully blocking manner—one script at a time.
    This ensures we do not overwhelm the system with too many simultaneous connections.
    """
    # Absolute or relative path to wherever your scripts live
    scripts_directory = '/app/kafka_producers'

    for script_name in PRODUCER_SCRIPTS:
        script_path: str = os.path.join(scripts_directory, script_name)

        if not os.path.isfile(script_path):
            logger.error(f"Script not found: {script_path}")
            continue

        command: str = f"python {script_path}"
        logger.info(f"Starting script: {script_name}")

        try:
            # subprocess.run waits until the script completes, making this call fully blocking.
            result = subprocess.run(command, shell=True)
            logger.info(
                f"Script {script_name} completed with exit code={result.returncode}."
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to run {script_name}. Error: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while running {script_name}: {e}")

if __name__ == '__main__':
    main()
