import os
import subprocess
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Maps producer scripts to their corresponding Kafka topics
producer_script_to_topic = {
    'allplays_producer.py': 'allplays_info',    
    'weather_producer.py': 'weather_info',
    'boxscores_producer.py': 'boxscore_info',
    'officials_producer.py': 'officials_info',
    'pitchers_producer.py': 'pitchers_info',
    'text_descriptions_producer.py': 'text_descriptions',
    'venue_producer.py': 'venue_info',
}

# Runs producer script with game number arguments
def start_puller_game_number(script_name, start_game, end_game, puller_id, topic, semaphore):
    script_path = os.path.join('/app/kafka_producers', script_name)
    if not os.path.isfile(script_path):  # Check if the script exists
        logger.error(f"Script not found: {script_path}")
        semaphore.release()
        return

    command = f"python {script_path} --start_game {start_game} --end_game {end_game} --topic {topic}"
    try:
        process = subprocess.Popen(command, shell=True)  # Execute the script
        process.wait()  # Wait for the process to complete
        logger.info(f"Started puller {puller_id} for games {start_game} to {end_game} using {script_name} with topic {topic}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start puller {puller_id} using {script_name}: {e}")
    except Exception as e:
        logger.error(f"An error occurred while starting puller {puller_id} using {script_name}: {e}")
    finally:
        semaphore.release()  # Release the semaphore

def main():
    parser = argparse.ArgumentParser(description="Start Pullers for MLB Data")
    parser.add_argument('start_game_number', type=int, help='Start game number')
    parser.add_argument('end_game_number', type=int, help='End game number')
    parser.add_argument('--script', type=str, help='Producer script name', required=True)
    args = parser.parse_args()

    start_game_number = args.start_game_number
    end_game_number = args.end_game_number
    script_name = args.script

    if script_name == 'gameresults_producer.py':
        logger.error("gameresults_producer.py should be run separately in the DAG.")
        return

    total_pullers = 100
    max_concurrent_pullers = 3
    game_range = end_game_number - start_game_number + 1
    games_per_puller = game_range // total_pullers

    semaphore = Semaphore(max_concurrent_pullers)
    kafka_topic = producer_script_to_topic.get(script_name)

    if not kafka_topic:
        logger.error(f"No Kafka topic found for script {script_name}.")
        return

    with ThreadPoolExecutor(max_workers=total_pullers) as executor:
        futures = []
        for i in range(total_pullers):
            start_game = start_game_number + i * games_per_puller
            end_game = start_game + games_per_puller - 1
            if i == total_pullers - 1:
                end_game = end_game_number

            semaphore.acquire()
            futures.append(executor.submit(start_puller_game_number, script_name, start_game, end_game, i+1, kafka_topic, semaphore))

        # Ensure to wait for all futures to complete before exiting
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
