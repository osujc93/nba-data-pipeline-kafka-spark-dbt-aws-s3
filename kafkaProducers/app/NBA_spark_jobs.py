import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Map each NBA Spark job to its corresponding Kafka topic
spark_job_to_topic = {
    'NBA_advanced_player_stats.py': 'NBA_advanced_player_stats',
    'NBA_advanced_team_stats.py': 'NBA_advanced_team_stats',
    'NBA_traditional_player_stats.py': 'NBA_traditional_player_stats',
    'NBA_traditional_team_stats.py': 'NBA_traditional_team_stats',
    'NBA_regular_season_standings.py': 'NBA_regular_season_standings'
}

def start_spark_job(script_name, topic):
    script_path = os.path.join('/app/spark_jobs', script_name)
    if not os.path.isfile(script_path):
        logger.error(f"Script not found: {script_path}")
        return

    command = f"python {script_path}"
    try:
        process = subprocess.Popen(command, shell=True)
        process.wait()
        logger.info(f"Started Spark job {script_name} for topic {topic}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start Spark job {script_name} for topic {topic}: {e}")
    except Exception as e:
        logger.error(f"An error occurred while starting Spark job {script_name} for topic {topic}: {e}")

def main():
    # Run all Spark jobs in parallel (no argument parser needed)
    with ThreadPoolExecutor(max_workers=len(spark_job_to_topic)) as executor:
        futures = []
        for script_name, topic in spark_job_to_topic.items():
            futures.append(executor.submit(start_spark_job, script_name, topic))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred: {e}")

    logger.info("All NBA Spark jobs have completed.")

if __name__ == '__main__':
    main()
