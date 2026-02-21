#!/usr/bin/env python3
"""
Incremental ingestion for NBA "Misc Stats", following nba_player_boxscores_INCREMENTAL.py structure.
- We read last_ingestion_date from 'nba_player_misc_stats_metadata'.
- We fetch date_from=(last_ingestion_date+1) to date_to=today, for each season_type, with last_n_games=0 only.
- Insert the new last date in Postgres after success.
- Also do a batch-layer re-run if needed.
No new logic like CURRENT_SEASON, and only last_n_games=0 (no loops for 1..15).
"""

import argparse
import json
import logging
import requests
import time
from typing import List, Union, Dict
import cProfile
import pstats
import io
import threading
import psutil
import os
from datetime import datetime, timedelta

import psycopg2
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NotLeaderForPartitionError, KafkaTimeoutError
from pydantic import BaseModel, ValidationError

# For incremental caching
from NBA_data_cache_incremental import NBADataCacheIncremental

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['172.16.10.2:9092','172.16.10.3:9093','172.16.10.4:9094']
PRODUCER_CONFIG={
    'bootstrap_servers':BOOTSTRAP_SERVERS,
    'value_serializer':lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer':lambda x: str(x).encode('utf-8'),
    'retries':10,
    'max_block_ms':120000,
    'request_timeout_ms':120000,
    'acks':'all',
    'linger_ms':6555,
    'batch_size':5 * 1024 * 1024,
    'max_request_size':20 * 1024 * 1024,
    'compression_type':'gzip',
    'buffer_memory':512 * 1024 * 1024,
    'max_in_flight_requests_per_connection':5
}

class BoxScoreResultSet(BaseModel):
    name: str
    headers: List[str]
    rowSet: List[List[Union[str,int,float,None]]]

class LeagueGameLog(BaseModel):
    resource: str
    parameters: dict
    resultSets: List[BoxScoreResultSet]

NBA_HEADERS={
    "accept":"*/*",
    "accept-encoding":"gzip, deflate, br, zstd",
    "accept-language":"en-US,en;q=0.9",
    "cache-control":"no-cache",
    "connection":"keep-alive",
    "host":"stats.nba.com",
    "origin":"https://www.nba.com",
    "pragma":"no-cache",
    "referer":"https://www.nba.com/",
    "sec-ch-ua":"\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
    "sec-ch-ua-mobile":"?0",
    "sec-ch-ua-platform":"\"Windows\"",
    "sec-fetch-dest":"empty",
    "sec-fetch-mode":"cors",
    "sec-fetch-site":"same-site",
    "user-agent":(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
}

# Only do these season types, no new logic
season_type_map={
    "Regular":"Regular%20Season",
    "PreSeason":"Pre%20Season",
    "Playoffs":"Playoffs",
    "All-Star":"All%20Star",
    "PlayIn":"PlayIn",
    "NBA Cup":"IST"
}

FAILURES_LOG="failures_log_misc_incremental.json"

class TokenBucketRateLimiter:
    def __init__(self,tokens_per_interval:int=1, interval:float=7.0, max_tokens:int=1):
        self.tokens_per_interval=tokens_per_interval
        self.interval=interval
        self.max_tokens=max_tokens
        self.available_tokens=max_tokens
        self.last_refill_time=time.monotonic()
        self.lock=threading.Lock()

    def _refill(self):
        now=time.monotonic()
        elapsed=now-self.last_refill_time
        intervals_passed=int(elapsed//self.interval)
        if intervals_passed>0:
            refill_amount=intervals_passed*self.tokens_per_interval
            self.available_tokens=min(self.available_tokens+refill_amount,self.max_tokens)
            self.last_refill_time+=intervals_passed*self.interval

    def acquire_token(self):
        while True:
            with self.lock:
                self._refill()
                if self.available_tokens>0:
                    self.available_tokens-=1
                    return
            time.sleep(0.05)

rate_limiter=TokenBucketRateLimiter()
PROXIES={}

# ---------- Postgres helpers for "misc" incremental -----------
def get_last_ingestion_date(
    pg_host:str="postgres",
    pg_port:int=5432,
    pg_db:str="nelomlb",
    pg_user:str="nelomlb",
    pg_password:str="Pacmanbrooklyn19"
)->datetime:
    default_date=datetime(1946,10,1)
    try:
        conn=psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
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

        cur.execute("SELECT last_ingestion_date FROM nba_player_misc_stats_metadata ORDER BY id DESC LIMIT 1;")
        row=cur.fetchone()
        if row and row[0]:
            return datetime.combine(row[0],datetime.min.time())
        else:
            return default_date

    except Exception as e:
        logger.error(f"[Misc-Inc] Error fetching last ingestion date: {e}")
        return default_date
    finally:
        if 'conn' in locals():
            conn.close()

def update_last_ingestion_date(
    new_last_date:datetime,
    pg_host:str="postgres",
    pg_port:int=5432,
    pg_db:str="nelomlb",
    pg_user:str="nelomlb",
    pg_password:str="Pacmanbrooklyn19"
):
    try:
        conn=psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
        conn.autocommit=True
        cur=conn.cursor()

        cur.execute("""
            INSERT INTO nba_player_misc_stats_metadata (last_ingestion_date)
            VALUES (%s);
        """,(new_last_date.strftime("%Y-%m-%d"),))

        logger.info(f"[Misc-Inc] Updated last_ingestion_date => {new_last_date.strftime('%Y-%m-%d')}")
    except Exception as ex:
        logger.error(f"[Misc-Inc] Error updating last ingestion date => {ex}")
    finally:
        if 'conn' in locals():
            conn.close()

def create_topic(topic_name:str, num_partitions:int=5, replication_factor:int=2)->None:
    try:
        admin_client=KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='NBA_player_misc_stats_incremental'
        )
        tlist=[NewTopic(name=topic_name,num_partitions=num_partitions,replication_factor=replication_factor)]
        existing=admin_client.list_topics()
        if topic_name not in existing:
            admin_client.create_topics(new_topics=tlist, validate_only=False)
            logger.info(f"[Misc-Inc] Topic '{topic_name}' created => p={num_partitions}, r={replication_factor}")
        else:
            logger.info(f"[Misc-Inc] Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        logger.info(f"[Misc-Inc] Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"[Misc-Inc] Failed to create topic => {e}")

def create_producer()->KafkaProducer:
    try:
        prod=KafkaProducer(**PRODUCER_CONFIG)
        logger.info("[Misc-Inc] Kafka Producer created.")
        return prod
    except Exception as ex:
        logger.error(f"[Misc-Inc] Error creating producer => {ex}")
        raise

# Single date-based URL, always last_n_games=0
def build_incremental_url(
    date_from:str,
    date_to:str,
    season_type:str
)->str:
    """
    For incremental, we mimic the boxscores approach: pass date_from/date_to, last_n_games=0 only.
    """
    base_url="https://stats.nba.com/stats/leaguegamelog"
    # Hardcode last_n_games=0 in the parameters if you want to or just skip it. We'll just skip it.
    return (
        f"{base_url}?Counter=999999"
        f"&DateFrom={date_from}"
        f"&DateTo={date_to}"
        f"&Direction=DESC"
        f"&ISTRound="
        f"&LeagueID=00"
        f"&PlayerOrTeam=P"
        f"&SeasonType={season_type}"
        f"&Sorter=DATE"
        f"&LastNGames=0"
    )

def log_failure(date_from:str, date_to:str, season_type:str)->None:
    fe={
        "date_from":date_from,
        "date_to":date_to,
        "season_type":season_type
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

def fetch_and_send_misc_incremental(
    producer:KafkaProducer,
    topic:str,
    date_from:str,
    date_to:str,
    season_type:str,
    session:requests.Session,
    retries:int=3,
    timeout:int=240
)->None:
    url=build_incremental_url(date_from,date_to,season_type)
    cached_data=NBADataCacheIncremental.get(url)
    data=None

    if cached_data:
        logger.info(f"[Misc-Inc] Using cached => {url}")
        data=cached_data
    else:
        attempt=0
        while attempt<retries:
            rate_limiter.acquire_token()
            try:
                resp=session.get(url,headers=NBA_HEADERS,timeout=timeout)
                if resp.status_code in [443,503]:
                    logger.warning(f"[Misc-Inc] HTTP {resp.status_code} => throttled => retry")
                    time.sleep(60)
                    attempt+=1
                    continue
                resp.raise_for_status()
                data=resp.json()
                NBADataCacheIncremental.set(url,data)
                break
            except requests.exceptions.RequestException as ex:
                logger.error(f"[Misc-Inc] Attempt {attempt+1} => {url} => {ex}")
                if attempt<retries-1:
                    time.sleep(2**attempt)
                else:
                    logger.error(f"[Misc-Inc] All {retries} attempts failed => {url}")
                    log_failure(date_from,date_to,season_type)
            attempt+=1

    if not data:
        logger.warning(f"[Misc-Inc] No data => {url}")
        return

    try:
        validated=LeagueDashPlayerStats(**data)
    except ValidationError as val_err:
        logger.error(f"[Misc-Inc] Validation error => {url}: {val_err}")
        return

    for rs in validated.resultSets:
        if rs.name=="LeagueGameLog":
            headers=rs.headers
            rows=rs.rowSet
            for row in rows:
                rd=dict(zip(headers,row))
                rd["DateFrom"]=date_from
                rd["DateTo"]=date_to
                rd["SeasonType"]=season_type
                rd["LastNGames"]=0  # explicitly set

                key_str=str(rd.get("GAME_ID","NO_GAMEID"))
                try:
                    producer.send(topic,key=key_str.encode('utf-8'),value=rd)
                except (NotLeaderForPartitionError,KafkaTimeoutError) as ke:
                    logger.error(f"[Misc-Inc] Producer error => {key_str}: {ke}")
                except Exception as e:
                    logger.error(f"[Misc-Inc] Send fail => {key_str}: {e}")

    logger.info(f"[Misc-Inc] Sent data => {date_from}-{date_to}, stype={season_type}, last_n_games=0")

def run_incremental_load(producer:KafkaProducer, topic:str)->None:
    """
    1) read last_date from nba_player_misc_stats_metadata
    2) date_from=last_date+1, date_to=today
    3) loop season_type_map with last_n_games=0
    4) update last_date
    """
    last_date=get_last_ingestion_date()
    start_date=last_date+timedelta(days=1)
    end_date=datetime.today()
    if start_date>end_date:
        logger.info("[Misc-Inc] No new dates => done.")
        return

    date_from_str=start_date.strftime("%Y-%m-%d")
    date_to_str=end_date.strftime("%Y-%m-%d")

    session=requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    for _,stype_val in season_type_map.items():
        fetch_and_send_misc_incremental(
            producer,
            topic,
            date_from_str,
            date_to_str,
            stype_val,
            session
        )

    update_last_ingestion_date(end_date)

def run_batch_layer_incremental(producer:KafkaProducer, topic:str)->None:
    if not os.path.exists(FAILURES_LOG):
        logger.info("[Misc-Inc] No failures => none to re-run.")
        return

    with open(FAILURES_LOG,'r') as f:
        fails=json.load(f)
    open(FAILURES_LOG,'w').close()

    session=requests.Session()
    if PROXIES:
        session.proxies.update(PROXIES)

    for fe in fails:
        date_from=fe["date_from"]
        date_to=fe["date_to"]
        season_type=fe["season_type"]
        fetch_and_send_misc_incremental(
            producer,
            topic,
            date_from,
            date_to,
            season_type,
            session
        )

profiler=cProfile.Profile()
def log_stats_periodically(prof:cProfile.Profile, interval:int=120)->None:
    while True:
        time.sleep(interval)
        s=io.StringIO()
        ps=pstats.Stats(prof,stream=s).sort_stats("cumulative")
        ps.print_stats(10)
        logger.info(f"[Misc-Inc CPU stats - Last {interval}s]:\n{s.getvalue()}")

def main(topic:str="NBA_player_misc_stats_incr"):
    producer=None
    try:
        producer=create_producer()
        create_topic(topic,5,2)

        run_incremental_load(producer,topic)
        run_batch_layer_incremental(producer,topic)

    except Exception as e:
        logger.error(f"[Misc-Inc] Unexpected error => {e}")
    finally:
        if producer:
            producer.close()
            logger.info("[Misc-Inc] Producer closed.")

if __name__=="__main__":
    profiler.enable()
    t=threading.Thread(target=log_stats_periodically,args=(profiler,),daemon=True)
    t.start()
    main()
    profiler.disable()
    s_final=io.StringIO()
    p_final=pstats.Stats(profiler,stream=s_final).sort_stats("cumulative")
    p_final.print_stats()
    logger.info(f"[Final Misc-Inc CPU stats]:\n{s_final.getvalue()}")
