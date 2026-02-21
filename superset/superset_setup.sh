#!/usr/bin/env python3
# File: superset_setup.sh
#
# This script runs advanced queries & visualizations setup for scd_player_boxscores
# in Spark Thrift (Hive), using Superset's Python client. Despite the .sh extension,
# the code below is Python. It is invoked similarly to a Python script (entrypoint).
#
# PEP 8 / PEP 257 / PEP 484 compliance:
# - 4-space indentation
# - Docstrings for clarity
# - Type hints where it makes sense

import os
import subprocess
import time
from typing import Optional

# We'll use supersetclient for programmatic creation of DBs/charts/dashboards
try:
    from supersetclient import Client
    from supersetclient.models import Database, Dataset, Chart, Dashboard
except ModuleNotFoundError:
    subprocess.run(["pip", "install", "supersetclient", "requests"], check=True)
    from supersetclient import Client
    from supersetclient.models import Database, Dataset, Chart, Dashboard


def main() -> None:
    """
    Main entrypoint for Superset setup. This includes:
      1) Installing/upgrading Superset if needed
      2) Initial DB migrations & admin user creation
      3) Connecting to Spark Thrift (Hive)
      4) Creating advanced charts/dashboards in Superset
      5) Starting Superset in the background
    """
    admin_username: str = os.getenv("ADMIN_USERNAME", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "admin")
    admin_firstname: str = os.getenv("ADMIN_FIRSTNAME", "Admin")
    admin_lastname: str = os.getenv("ADMIN_LASTNAME", "User")
    admin_email: str = os.getenv("ADMIN_EMAIL", "admin@example.com")
    superset_url: str = "http://localhost:8098"

    print("[Superset Setup] Installing/Upgrading apache-superset if needed ...")
    subprocess.run(["pip", "install", "--upgrade", "apache-superset[hive,trino]"], check=True)

    print("[Superset Setup] Upgrading the Superset DB ...")
    subprocess.run(["superset", "db", "upgrade"], check=True)

    print("[Superset Setup] Creating admin user ...")
    subprocess.run([
        "superset", "fab", "create-admin",
        "--username", admin_username,
        "--firstname", admin_firstname,
        "--lastname", admin_lastname,
        "--email", admin_email,
        "--password", admin_password
    ], check=True)

    print("[Superset Setup] Running superset init ...")
    subprocess.run(["superset", "init"], check=True)

    print("[Superset Setup] Starting Superset server on :8098 in background ...")
    server_proc = subprocess.Popen(["gunicorn", "-b", "0.0.0.0:8098", "superset.app:create_app()"])
    time.sleep(5)

    spark_thrift_uri: str = os.getenv(
        "SPARK_THRIFT_URI",
        "hive://sparkuser@spark-thriftserver:10015/default?auth=NOSASL"
    )
    db_name: str = "Spark Thrift (NBA)"

    print(f"[Superset Setup] Connecting to Superset at {superset_url} as admin ...")
    client = Client(baseurl=superset_url, username=admin_username, password=admin_password)

    print(f"[Superset Setup] Creating DB: {db_name}")
    db_obj: Database = Database(
        name=db_name,
        sqlalchemy_uri=spark_thrift_uri
    )
    client.add_database(db_obj)

    dataset_name: str = "scd_player_boxscores"
    print(f"[Superset Setup] Adding dataset for table: {dataset_name}")
    dataset_obj: Dataset = Dataset(
        table_name=dataset_name,
        database_id=db_obj.id,
        schema="default"
    )
    client.add_dataset(dataset_obj)

    charts_list = []

    chart_top_5_sql: str = """
        SELECT player_id,
               player_name,
               season,
               points,
               game_id,
               game_date,
               rank
        FROM (
            SELECT 
                player_id,
                player_name,
                season,
                points,
                game_id,
                game_date,
                RANK() OVER (
                    PARTITION BY player_id, season
                    ORDER BY points DESC
                ) AS rank
            FROM scd_player_boxscores
        ) t
        WHERE rank <= 5
        ORDER BY player_id, season, rank
    """
    chart_top_5: Chart = Chart(
        slice_name="(1) Top 5 Games by Points",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_top_5_sql,
            "custom_sql": chart_top_5_sql,
            "row_limit": 5000
        },
    )
    charts_list.append(chart_top_5)

    chart_avg_stats_sql: str = """
        SELECT
            player_id,
            player_name,
            season,
            ROUND(AVG(points), 2) AS avg_points,
            ROUND(AVG(rebounds), 2) AS avg_rebounds,
            ROUND(AVG(assists), 2) AS avg_assists,
            ROUND(AVG(field_goal_percentage), 2) AS avg_fg_pct,
            COUNT(DISTINCT game_id) AS games_played
        FROM scd_player_boxscores
        GROUP BY player_id, player_name, season
        ORDER BY player_id, season
    """
    chart_avg_stats: Chart = Chart(
        slice_name="(2) Avg Stats by Player/Season",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_avg_stats_sql,
            "custom_sql": chart_avg_stats_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_avg_stats)

    chart_double_triple_sql: str = """
        SELECT 
            player_id,
            player_name,
            game_id,
            game_date,
            points,
            rebounds,
            assists,
            CASE 
                WHEN (points >= 10 AND rebounds >= 10 AND assists >= 10) THEN 'Triple-Double'
                WHEN (
                      (points >= 10 AND rebounds >= 10)
                      OR (points >= 10 AND assists >= 10)
                      OR (rebounds >= 10 AND assists >= 10)
                     ) THEN 'Double-Double'
                ELSE 'None'
            END AS double_or_triple
        FROM scd_player_boxscores
        WHERE season_type = 'Regular Season'
        ORDER BY game_date
    """
    chart_double_triple: Chart = Chart(
        slice_name="(3) Double-Double / Triple-Double",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_double_triple_sql,
            "custom_sql": chart_double_triple_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_double_triple)

    chart_teams_sql: str = """
        SELECT
            season,
            team_abbreviation,
            ROUND(AVG(points), 2) AS avg_points_scored_per_game
        FROM scd_player_boxscores
        WHERE season_type = 'Regular Season'
        GROUP BY season, team_abbreviation
        ORDER BY season, avg_points_scored_per_game DESC
    """
    chart_teams: Chart = Chart(
        slice_name="(4) Highest Scoring Teams by Season",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_teams_sql,
            "custom_sql": chart_teams_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_teams)

    chart_efficiency_sql: str = """
        WITH shooting AS (
            SELECT 
                player_id,
                player_name,
                season,
                SUM(field_goals_made) AS fg_made,
                SUM(field_goals_attempted) AS fg_attempted
            FROM scd_player_boxscores
            WHERE season_type = 'Regular Season'
            GROUP BY player_id, player_name, season
        )
        SELECT
            player_id,
            player_name,
            season,
            fg_made,
            fg_attempted,
            ROUND(fg_made * 1.0 / fg_attempted, 3) AS fg_pct
        FROM shooting
        WHERE fg_attempted >= 300
        ORDER BY fg_pct DESC
        LIMIT 20
    """
    chart_efficiency: Chart = Chart(
        slice_name="(5) Most Efficient Shooters",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_efficiency_sql,
            "custom_sql": chart_efficiency_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_efficiency)

    chart_rolling_sql: str = """
        SELECT
            player_id,
            player_name,
            game_date,
            points,
            AVG(points) OVER (
                PARTITION BY player_id
                ORDER BY TO_DATE(game_date) ASC
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS rolling_5_game_avg
        FROM scd_player_boxscores
        WHERE season_type = 'Regular Season'
        ORDER BY player_id, TO_DATE(game_date)
    """
    chart_rolling_avg: Chart = Chart(
        slice_name="(6) 5-Game Rolling Points Avg",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_rolling_sql,
            "custom_sql": chart_rolling_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_rolling_avg)

    chart_pivot_sql: str = """
        SELECT
            player_id,
            player_name,
            SUM(CASE WHEN win_loss = 'W' THEN points ELSE 0 END) AS total_points_in_wins,
            SUM(CASE WHEN win_loss = 'W' THEN 1 ELSE 0 END)      AS games_won,
            SUM(CASE WHEN win_loss = 'L' THEN points ELSE 0 END) AS total_points_in_losses,
            SUM(CASE WHEN win_loss = 'L' THEN 1 ELSE 0 END)      AS games_lost
        FROM scd_player_boxscores
        WHERE season_type = 'Regular Season'
        GROUP BY player_id, player_name
        ORDER BY player_id
    """
    chart_pivot: Chart = Chart(
        slice_name="(7) Points/Outcomes Pivot",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_pivot_sql,
            "custom_sql": chart_pivot_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_pivot)

    chart_monthly_sql: str = """
        SELECT 
          player_id,
          player_name,
          year,
          month,
          ROUND(AVG(points),2) as avg_points,
          ROUND(AVG(field_goal_percentage),3) as avg_fg_pct,
          COUNT(DISTINCT game_id) as games_played
        FROM scd_player_boxscores
        WHERE season_type = 'Regular Season'
        GROUP BY player_id, player_name, year, month
        ORDER BY player_id, year, month
    """
    chart_monthly: Chart = Chart(
        slice_name="(8) Monthly Scoring Summary",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_monthly_sql,
            "custom_sql": chart_monthly_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_monthly)

    chart_shot_breakdown_sql: str = """
        SELECT
          player_id,
          player_name,
          season,
          SUM(three_point_field_goals_attempted) AS total_3pt_att,
          SUM(three_point_field_goals_made)      AS total_3pt_made,
          ROUND(SUM(three_point_field_goals_made)*1.0 / NULLIF(SUM(three_point_field_goals_attempted),0), 3) AS pct_3pt,
          SUM(field_goals_attempted - three_point_field_goals_attempted) AS total_2pt_att,
          SUM(field_goals_made - three_point_field_goals_made)           AS total_2pt_made,
          ROUND(
            (SUM(field_goals_made - three_point_field_goals_made)*1.0) / 
            NULLIF(SUM(field_goals_attempted - three_point_field_goals_attempted),0), 
          3) AS pct_2pt
        FROM scd_player_boxscores
        WHERE season_type='Regular Season'
        GROUP BY player_id, player_name, season
        ORDER BY season, player_id
    """
    chart_shot_breakdown: Chart = Chart(
        slice_name="(9) 3PT vs 2PT Breakdown",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_shot_breakdown_sql,
            "custom_sql": chart_shot_breakdown_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_shot_breakdown)

    chart_30_plus_sql: str = """
        SELECT
            player_id,
            player_name,
            season,
            SUM(CASE WHEN points >= 30 THEN 1 ELSE 0 END) AS games_30plus,
            SUM(CASE WHEN points >= 40 THEN 1 ELSE 0 END) AS games_40plus,
            SUM(CASE WHEN points >= 50 THEN 1 ELSE 0 END) AS games_50plus
        FROM scd_player_boxscores
        WHERE season_type='Regular Season'
        GROUP BY player_id, player_name, season
        ORDER BY season, games_30plus DESC
    """
    chart_30_plus: Chart = Chart(
        slice_name="(10) Games of 30+, 40+, 50+ Points",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_30_plus_sql,
            "custom_sql": chart_30_plus_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_30_plus)

    chart_longest_win_streak_sql: str = """
        WITH team_games AS (
          SELECT DISTINCT
            team_id,
            team_name,
            game_id,
            game_date,
            win_loss
          FROM scd_player_boxscores
          WHERE season_type='Regular Season'
        ),
        team_sorted AS (
            SELECT
                team_id,
                team_name,
                game_id,
                game_date,
                win_loss,
                ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY TO_DATE(game_date)) as seq_in_season,
                SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END)
                  OVER (PARTITION BY team_id ORDER BY TO_DATE(game_date)
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_losses
            FROM team_games
        ),
        calc AS (
            SELECT
              team_id,
              team_name,
              game_id,
              game_date,
              seq_in_season,
              win_loss,
              cum_losses,
              seq_in_season - cum_losses AS group_identifier
            FROM team_sorted
            WHERE win_loss='W'
        )
        SELECT
          team_id,
          team_name,
          COUNT(*) AS consecutive_wins,
          MIN(game_date) AS start_date,
          MAX(game_date) AS end_date
        FROM calc
        GROUP BY team_id, team_name, group_identifier
        ORDER BY consecutive_wins DESC
        LIMIT 20
    """
    chart_longest_win_streak: Chart = Chart(
        slice_name="(11) Longest Win Streak by Team",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_longest_win_streak_sql,
            "custom_sql": chart_longest_win_streak_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_longest_win_streak)

    chart_top_defenders_sql: str = """
        SELECT
          player_id,
          player_name,
          season,
          SUM(blocks) AS total_blocks,
          SUM(steals) AS total_steals,
          (SUM(blocks) + SUM(steals)) AS total_def_plays
        FROM scd_player_boxscores
        WHERE season_type='Regular Season'
        GROUP BY player_id, player_name, season
        ORDER BY total_def_plays DESC
        LIMIT 50
    """
    chart_top_defenders: Chart = Chart(
        slice_name="(12) Top Defenders (Blocks+Steals)",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_top_defenders_sql,
            "custom_sql": chart_top_defenders_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_top_defenders)

    chart_ftr_sql: str = """
        SELECT
          player_id,
          player_name,
          season,
          SUM(free_throws_attempted) AS total_fta,
          SUM(field_goals_attempted) AS total_fga,
          ROUND(SUM(free_throws_attempted)*1.0 / NULLIF(SUM(field_goals_attempted),0), 3) AS ftr
        FROM scd_player_boxscores
        WHERE season_type='Regular Season'
        GROUP BY player_id, player_name, season
        ORDER BY ftr DESC
        LIMIT 50
    """
    chart_ftr: Chart = Chart(
        slice_name="(13) Free Throw Rate",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_ftr_sql,
            "custom_sql": chart_ftr_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_ftr)

    chart_perf_wins_losses_sql: str = """
        SELECT
          player_id,
          player_name,
          ROUND(AVG(CASE WHEN win_loss='W' THEN points END),2) as avg_points_in_wins,
          ROUND(AVG(CASE WHEN win_loss='L' THEN points END),2) as avg_points_in_losses,
          ROUND(AVG(CASE WHEN win_loss='W' THEN field_goal_percentage END),3) as avg_fg_pct_in_wins,
          ROUND(AVG(CASE WHEN win_loss='L' THEN field_goal_percentage END),3) as avg_fg_pct_in_losses
        FROM scd_player_boxscores
        WHERE season_type='Regular Season'
        GROUP BY player_id, player_name
        ORDER BY player_id
    """
    chart_perf_wins_losses: Chart = Chart(
        slice_name="(14) Performance in Wins vs. Losses",
        viz_type="table",
        datasource_id=dataset_obj.id,
        datasource_type="table",
        params={
            "query": chart_perf_wins_losses_sql,
            "custom_sql": chart_perf_wins_losses_sql,
            "row_limit": 50000
        },
    )
    charts_list.append(chart_perf_wins_losses)

    for c in charts_list:
        print(f"[Superset Setup] Adding chart: {c.slice_name}")
        client.add_chart(c)

    dashboard_obj: Dashboard = Dashboard(
        dashboard_title="NBA Stats Dashboard",
        position_json="{}"
    )
    dashboard_obj.slices = charts_list

    print("[Superset Setup] Creating 'NBA Stats Dashboard' ...")
    client.add_dashboard(dashboard_obj)

    print("[Superset Setup] All queries, charts, and dashboard created successfully!")
    print("[Superset Setup] Superset is running. Logs below ...")

    server_proc.wait()


if __name__ == "__main__":
    main()
