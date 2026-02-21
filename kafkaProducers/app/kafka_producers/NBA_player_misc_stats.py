#!/usr/bin/env python3
"""
Full ingestion for NBA "Misc Stats" that mimics nba_player_boxscores.py:
1) Fetch data in monthly chunks (month=0..9) for each season + season_type,
   but only uses last_n_games=0.
2) Send each chunk to Kafka.
3) Batch re-run any failures.
4) Store final ingestion date in Postgres (nba_player_misc_stats_metadata).
"""

import argparse
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Union, Dict, Any
import cProfile
import pstats
import io
import threading
import psutil
import os
from datetime import datetime

import psycopg2  # For finalizing ingestion date in Postgres
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NotLeaderForPartitionError, KafkaTimeoutError
from pydantic import BaseModel, ValidationError

# We'll import an in-memory cache to reduce repeated downloads
from NBA_data_cache import NBADataCache

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------ Kafka Config ------------------
BOOTSTRAP_SERVERS = ['172.16.10.2:9092','172.16.10.3:9093','172.16.10.4:9094']
PRODUCER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: str(x).encode('utf-8'),
    'retries': 10,
    'max_block_ms': 120000,
    'request_timeout_ms': 120000,
    'acks': 'all',
    'linger_ms': 6555,
    'batch_size': 5 * 1024 * 1024,
    'max_request_size': 20 * 1024 * 1024,
    'compression_type': 'gzip',
    'buffer_memory': 512 * 1024 * 1024,
    'max_in_flight_requests_per_connection': 5
}

# ------------------ Pydantic Models ------------------
class NBAPlayerResultSet(BaseModel):
    name: str
    headers: List[str]
    rowSet: List[List[Union[str,int,float,None]]]

class LeagueDashPlayerStats(BaseModel):
    resource: str
    parameters: dict
    resultSets: List[NBAPlayerResultSet]

# ------------------ NBA Config ------------------
NBA_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "connection": "keep-alive",
    "host": "stats.nba.com",
    "origin": "https://www.nba.com",
    "pragma": "no-cache",
    "referer": "https://www.nba.com/",
    "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

season_type_map = {
    "Regular": "Regular%20Season",
    "PreSeason": "Pre%20Season",
    "Playoffs": "Playoffs",
    "All-Star": "All%20Star",
    "PlayIn": "PlayIn",
    "NBA Cup": "IST"
}

seasons = [f"{y}-{str(y+1)[-2:]}" for y in range(1996,2025)]
per_modes = ["PerGame"]
LAST_N_GAMES = 0  # Only pulling data for last_n_games=0 now
FAILURES_LOG = "failures_log_misc.json"
MONTHS_0_TO_9 = list(range(10))

# ------------------ Token Bucket Rate Limiter ------------------
class TokenBucketRateLimiter:
    def __init__(self, tokens_per_interval=1, interval=8.0, max_tokens=1):
        self.tokens_per_interval = tokens_per_interval
        self.interval = interval
        self.max_tokens = max_tokens
        self.available_tokens = max_tokens
        self.last_refill_time = time.monotonic()
        self.lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        intervals_passed = int(elapsed // self.interval)
        if intervals_passed>0:
            refill_amount = intervals_passed*self.tokens_per_interval
            self.available_tokens = min(self.available_tokens + refill_amount, self.max_tokens)
            self.last_refill_time += intervals_passed*self.interval

    def acquire_token(self):
        while True:
            with self.lock:
                self._refill()
                if self.available_tokens>0:
                    self.available_tokens-=1
                    return
            time.sleep(0.05)

rate_limiter = TokenBucketRateLimiter()

def create_topic(topic_name:str, num_partitions=25, replication_factor=2):
    start_cpu_time=time.process_time()
    start_cpu_percent=psutil.cpu_percent()
    try:
        admin_client=KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='NBA_player_misc_stats')
        tlist=[NewTopic(name=topic_name,num_partitions=num_partitions,replication_factor=replication_factor)]
        existing=admin_client.list_topics()
        if topic_name not in existing:
            admin_client.create_topics(new_topics=tlist, validate_only=False)
            logger.info(
                f"Topic '{topic_name}' created successfully => partitions={num_partitions}, replication={replication_factor}"
            )
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create Kafka topic => {e}")
    finally:
        end_cpu_time=time.process_time()
        end_cpu_percent=psutil.cpu_percent()
        logger.info(
            f"CPU usage start:{start_cpu_percent}%, end:{end_cpu_percent}%. "
            f"CPU time for create_topic(): {end_cpu_time - start_cpu_time:.4f}s"
        )

def create_producer()->KafkaProducer:
    start_cpu=time.process_time()
    start_cpu_percent=psutil.cpu_percent()
    try:
        producer=KafkaProducer(**PRODUCER_CONFIG)
        logger.info("Kafka Producer created successfully (Player-Misc).")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer => {e}")
        raise
    finally:
        end_cpu=time.process_time()
        end_cpu_percent=psutil.cpu_percent()
        logger.info(
          f"CPU usage start:{start_cpu_percent}%, end:{end_cpu_percent}%. "
          f"CPU time for create_producer(): {end_cpu - start_cpu:.4f}s"
        )

def build_single_month_url(
    season:str,
    season_type:str,
    month:int,
    per_mode:str,
    last_n_games:int
)->str:
    base_url="https://stats.nba.com/stats/leaguedashplayerstats"
    return (
        f"{base_url}?"
        f"College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear="
        f"&GameScope=&GameSegment=&Height=&ISTRound="
        f"&LastNGames={last_n_games}&LeagueID=00&Location=&MeasureType=Misc"
        f"&Month={month}"
        f"&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N"
        f"&PerMode={per_mode}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N"
        f"&Season={season}&SeasonSegment=&SeasonType={season_type}"
        f"&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision="
        f"&Weight=&Counter=999999"
    )

def log_failure(season:str, season_type:str, per_mode:str, last_n_games:int):
    fe={
        "season":season,
        "season_type":season_type,
        "per_mode":per_mode,
        "last_n_games":last_n_games
    }
    if not os.path.exists(FAILURES_LOG):
        with open(FAILURES_LOG,'w') as f:
            json.dump([fe],f,indent=2)
    else:
        with open(FAILURES_LOG,'r') as f:
            data=json.load(f)
        data.append(fe)
        with open(FAILURES_LOG,'w') as f:
            json.dump(data,f,indent=2)

def fetch_single_month_data(url:str, session:requests.Session, retries:int=3, timeout:int=120)->Union[Dict[str,Any],None]:
    cached=NBADataCache.get(url)
    if cached:
        logger.info(f"[CACHE] => {url}")
        return cached
    data=None
    for attempt in range(retries):
        rate_limiter.acquire_token()
        try:
            resp=session.get(url,headers=NBA_HEADERS,timeout=timeout)
            if resp.status_code in [429,503]:
                logger.warning(f"Got HTTP {resp.status_code} => throttled => sleeping 60s.")
                time.sleep(60)
                continue
            resp.raise_for_status()
            data=resp.json()
            NBADataCache.set(url,data)
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt+1} => {url} => {e}")
            if attempt<retries-1:
                stime=2**attempt
                logger.info(f"Retrying in {stime} seconds...")
                time.sleep(stime)
            else:
                data=None
    return data

def fetch_and_send_player_data_all_months(
    producer:KafkaProducer,
    topic:str,
    season:str,
    season_type:str,
    per_mode:str,
    last_n_games:int
):
    """
    For the full ingestion approach, we do month=0..9, but only last_n_games=0.
    """
    start_cpu_time=time.process_time()
    start_cpu_percent=psutil.cpu_percent()

    session=requests.Session()
    combined_headers:List[str]=[]
    combined_rows:List[List[Union[str,int,float,None]]]=[]

    for m in MONTHS_0_TO_9:
        url=build_single_month_url(season,season_type,m,per_mode,last_n_games)
        single_data=fetch_single_month_data(url, session)
        if not single_data:
            logger.warning(f"No data from month={m} => failing entire 'all-months' for {season}")
            log_failure(season,season_type,per_mode,last_n_games)
            return
        # validate
        try:
            validated=LeagueDashPlayerStats(**single_data)
        except ValidationError as e:
            logger.error(f"Validation error => M={m}, {season}, {season_type}, {per_mode}, L{last_n_games}: {e}")
            log_failure(season,season_type,per_mode,last_n_games)
            return
        # merge rows
        for rs in validated.resultSets:
            if rs.name=="LeagueDashPlayerStats":
                if not combined_headers:
                    combined_headers=rs.headers
                combined_rows.extend(rs.rowSet)
                break

    if not combined_rows:
        logger.warning(f"Empty merges => {season}, {season_type}, {per_mode}, L{last_n_games}")
        return

    # produce once
    for row in combined_rows:
        rd=dict(zip(combined_headers,row))
        rd["Season"]=season
        rd["SeasonType"]=season_type
        rd["Months"]="0..9 (combined)"
        rd["PerMode"]=per_mode
        rd["LastNGames"]=last_n_games

        pid=rd.get("PLAYER_ID","0")
        try:
            producer.send(topic, key=str(pid).encode('utf-8'), value=rd)
        except (NotLeaderForPartitionError, KafkaTimeoutError) as ex:
            logger.error(f"Producer error => pid={pid}: {ex}")
        except Exception as ex:
            logger.error(f"Failed => pid={pid}: {ex}")

    logger.info(f"[SUCCESS] All-months combined => {season}, {season_type}, {per_mode}, L{last_n_games}")
    end_cpu_time=time.process_time()
    end_cpu_percent=psutil.cpu_percent()
    logger.info(
      f"CPU usage start:{start_cpu_percent}%, end:{end_cpu_percent}%. "
      f"CPU time => {end_cpu_time - start_cpu_time:.4f}s"
    )

def process_all_misc_combinations(producer:KafkaProducer, topic:str, max_workers:int=1):
    """
    This is the full ingestion approach for "Misc Stats."
    We'll do season x season_type x per_mode x last_n_games=0, chunked by month=0..9.
    """
    start_cpu_time=time.process_time()
    start_cpu_percent=psutil.cpu_percent()

    tasks=[]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for season_str in seasons:
            for _, stype_val in season_type_map.items():
                for pm in per_modes:
                    # Only last_n_games=0 now:
                    future = executor.submit(
                        fetch_and_send_player_data_all_months,
                        producer,
                        topic,
                        season_str,
                        stype_val,
                        pm,
                        0  # only 0
                    )
                    tasks.append(future)

        for fut in as_completed(tasks):
            try:
                fut.result()
            except Exception as e:
                logger.error(f"[Misc-Full] Worker error => {e}")

    end_cpu_time=time.process_time()
    end_cpu_percent=psutil.cpu_percent()
    logger.info(
      f"CPU usage start:{start_cpu_percent}%, end:{end_cpu_percent}%. CPU => {end_cpu_time - start_cpu_time:.4f}s"
    )

def run_batch_layer(producer:KafkaProducer, topic:str):
    """
    Re-run any monthly combos that failed.
    """
    if not os.path.exists(FAILURES_LOG):
        logger.info("No failures => no batch re-run for Player Misc.")
        return
    logger.info("Starting batch re-run => 'NBA_player_misc_stats' from failures log..")
    with open(FAILURES_LOG,'r') as f:
        fails=json.load(f)
    # Clear to avoid infinite loop
    open(FAILURES_LOG,'w').close()

    for fe in fails:
        season=fe["season"]
        st=fe["season_type"]
        pm=fe["per_mode"]
        lng=fe["last_n_games"]
        fetch_and_send_player_data_all_months(
            producer,
            topic,
            season,
            st,
            pm,
            lng
        )

    logger.info("Batch re-run completed => 'NBA_player_misc_stats'.")

def finalize_batch_in_postgres():
    """
    Insert today's date into nba_player_misc_stats_metadata, same approach as boxscores.py
    """
    final_date_ingested = datetime.today()
    try:
        conn = psycopg2.connect(
            dbname="nelomlb",
            user="nelomlb",
            password="Pacmanbrooklyn19",
            host="postgres",
            port=5432
        )
        conn.autocommit=True
        cur=conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nba_player_misc_stats_metadata (
                id SERIAL PRIMARY KEY,
                last_ingestion_date DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute(
            """
            INSERT INTO nba_player_misc_stats_metadata (last_ingestion_date)
            VALUES (%s);
            """,
            (final_date_ingested.strftime("%Y-%m-%d"),)
        )
        logger.info("[FINALIZE] Inserted final date %s into nba_player_misc_stats_metadata.",
                    final_date_ingested.strftime('%Y-%m-%d'))
    except Exception as exc:
        logger.error("[FINALIZE] Error inserting final ingestion date for Misc Stats: %s", exc)
    finally:
        if 'conn' in locals():
            conn.close()

profiler=cProfile.Profile()
def log_stats_periodically(prof: cProfile.Profile, interval=120)->None:
    while True:
        time.sleep(interval)
        s=io.StringIO()
        ps=pstats.Stats(prof, stream=s).sort_stats("cumulative")
        ps.print_stats(10)
        logger.info(f"[Misc CPU stats - Last {interval}s]:\n{s.getvalue()}")

def main(topic: str="NBA_player_misc_stats"):
    producer=None
    try:
        start_cpu_main=time.process_time()
        start_cpu_percent_main=psutil.cpu_percent()

        producer=create_producer()
        create_topic(topic,25,2)

        # Full ingestion
        process_all_misc_combinations(producer, topic, max_workers=1)

        # Batch re-run
        run_batch_layer(producer, topic)

        # Finalize in Postgres
        finalize_batch_in_postgres()

        end_cpu_time_main=time.process_time()
        end_cpu_percent_main=psutil.cpu_percent()
        logger.info(
          f"CPU usage start:{start_cpu_percent_main}%, end:{end_cpu_percent_main}%. "
          f"CPU time(main) => {end_cpu_time_main - start_cpu_main:.4f}s"
        )
        logger.info("All player 'Misc' data done (full).")
    except Exception as e:
        logger.error(f"[Misc-Full] Unexpected error => {e}")
    finally:
        if producer:
            try:
                producer.close()
                logger.info("Kafka Producer closed (Player-Misc).")
            except Exception as ex:
                logger.error(f"Error closing => {ex}")

if __name__=="__main__":
    profiler.enable()
    t=threading.Thread(target=log_stats_periodically,args=(profiler,),daemon=True)
    t.start()
    main()
    profiler.disable()
    s_final=io.StringIO()
    p_final=pstats.Stats(profiler,stream=s_final).sort_stats("cumulative")
    p_final.print_stats()
    logger.info(f"[Final CPU profiling stats]:\n{s_final.getvalue()}")
