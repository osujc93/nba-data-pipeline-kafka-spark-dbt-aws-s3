"""
This script manages the parallel execution of multiple Spark jobs to consume and process MLB game data from Kafka topics using a thread pool. 

Each job focuses on different aspects of the game data, such as box scores, player information, and game results. 

A dictionary maps each Spark job script to its corresponding Kafka topic, allowing the script to dynamically determine the correct Kafka topic for each execution. 

It initializes a ThreadPoolExecutor with the number of worker threads equal to the number of topics to consume from. 
"""

import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Map each Spark job to its corresponding Kafka topic
spark_job_to_topic = {
    'gameresults_spark_job.py': 'game_results', 
    'boxscore_spark_job.py': 'boxscore_info'
}

# Start a Spark job for a given Kafka topic
def start_spark_job(spark_job, topic):
    script_path = os.path.join('/app/spark_jobs', spark_job)
    if not os.path.isfile(script_path):  # Check if the script exists
        logger.error(f"Script not found: {script_path}")
        return

    command = f"python {script_path} --topic {topic}"
    try:
        process = subprocess.Popen(command, shell=True)  # Execute the Spark job
        process.wait()  # Wait for the process to complete
        logger.info(f"Started Spark job {spark_job} for topic {topic}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start Spark job {spark_job} for topic {topic}: {e}")
    except Exception as e:
        logger.error(f"An error occurred while starting Spark job {spark_job} for topic {topic}: {e}")

# Main function to parse arguments and run Spark jobs
def main():
    parser = argparse.ArgumentParser(description="Start Spark Consumers for MLB Data")
    parser.add_argument('--topics', nargs='+', help='Kafka topics to consume from', required=True)
    args = parser.parse_args()

    topics = args.topics

    # Initialize a ThreadPoolExecutor to run Spark jobs concurrently
    with ThreadPoolExecutor(max_workers=len(topics)) as executor:
        futures = []
        for spark_job, topic in spark_job_to_topic.items():  # Loop over Spark jobs and topics
            if topic in topics:
                futures.append(executor.submit(start_spark_job, spark_job, topic))

        # Process futures as they complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
