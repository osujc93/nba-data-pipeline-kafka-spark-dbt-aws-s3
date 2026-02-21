"""
This script manages the parallel execution of multiple embedding scripts to process MLB game data. 

Each script generates embeddings for different aspects of the game data, such as all plays, box scores, game results, and player information. 

It ensures that the specified embedding scripts run concurrently using a thread pool.

These scripts handle various types of game data and generate embeddings, which are useful for machine learning models and data analysis.

The script includes a function that starts a given embedding script by constructing the script path and verifying its existence. 

It constructs a command to run the script and executes it as a subprocess, logging the outcome.

It initializes a ThreadPoolExecutor with the number of worker threads equal to the number of scripts to run. 

The main function submits tasks to the executor, waits for their completion, and handles any exceptions that occur during execution.

This approach ensures efficient resource usage and parallel processing of the embedding scripts, allowing for scalable and maintainable data processing.
"""

import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of embedding scripts to be run
embedding_scripts = [
    'allplaysinfo_embeddings.py',
    'boxscores_embeddings.py',
    'gameresults_embeddings.py',
    'iceberg_tables_embeddings.py',
    'officialsinfo_embeddings.py',
    'pitchersinfo_embeddings.py',
    'playersinfo_embeddings.py',
    'text_descriptions_embeddings.py',
    'venueinfo_to_embeddings.py',
    'weatherinfo_to_embeddings.py'
]

# Function to start a given embedding script
def start_embedding_script(script_name):
    script_path = os.path.join('/app/embeddings', script_name)
    if not os.path.isfile(script_path):
        logger.error(f"Script not found: {script_path}")
        return

    command = f"python {script_path}"
    try:
        process = subprocess.Popen(command, shell=True)
        process.wait()
        logger.info(f"Started embedding script {script_name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start embedding script {script_name}: {e}")
    except Exception as e:
        logger.error(f"An error occurred while starting embedding script {script_name}: {e}")

# Main function to parse arguments and run embedding scripts
def main():
    parser = argparse.ArgumentParser(description="Start Embedding Scripts for MLB Data")
    parser.add_argument('--scripts', nargs='+', help='Embedding scripts to run', required=True)
    args = parser.parse_args()

    scripts_to_run = args.scripts

    # Initialize a ThreadPoolExecutor to run scripts concurrently
    with ThreadPoolExecutor(max_workers=len(scripts_to_run)) as executor:
        futures = []
        for script_name in embedding_scripts:
            if script_name in scripts_to_run:
                futures.append(executor.submit(start_embedding_script, script_name))

        # Process futures as they complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
