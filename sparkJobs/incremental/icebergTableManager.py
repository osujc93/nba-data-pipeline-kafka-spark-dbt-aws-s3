#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class IcebergTableManager:
    """
    Manages the creation or verification of the main boxscore table in Iceberg
    for incremental loads.
    """

    def __init__(self, spark: SparkSession):
        # Directly set spark since all stats/profiling code has been removed
        self.spark = spark

    def create_main_boxscore_table_iceberg(self) -> None:
        """
        Create or ensure existence of the main Iceberg table for NBA player boxscores.
        """
        try:
            self.spark.sql(
                """
                CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_nba_player_boxscores.nba_player_boxscores (
                    season_id STRING,
                    player_id INT,
                    player_name STRING,
                    team_id INT,
                    team_abbreviation STRING,
                    team_name STRING,
                    game_id STRING,
                    game_date DATE,
                    year INT,
                    month INT,
                    day INT,
                    matchup STRING,
                    win_loss STRING,
                    minutes_played INT,
                    field_goals_made INT,
                    field_goals_attempted INT,
                    field_goal_percentage DOUBLE,
                    three_point_field_goals_made INT,
                    three_point_field_goals_attempted INT,
                    three_point_field_goal_percentage DOUBLE,
                    free_throws_made INT,
                    free_throws_attempted INT,
                    free_throw_percentage DOUBLE,
                    offensive_rebounds INT,
                    defensive_rebounds INT,
                    rebounds INT,
                    assists INT,
                    steals INT,
                    blocks INT,
                    turnovers INT,
                    personal_fouls INT,
                    points INT,
                    plus_minus DOUBLE,
                    fantasy_points DOUBLE,
                    video_available INT,
                    season STRING,
                    season_type STRING,
                    game_date_param STRING
                )
                USING ICEBERG
                PARTITIONED BY (season, season_type)
                """
            )
            logger.info("[Incremental] Iceberg table verified/created.")
        except Exception as e:
            logger.error("[Incremental] Error creating main boxscore table: %s", e, exc_info=True)
            sys.exit(1)
