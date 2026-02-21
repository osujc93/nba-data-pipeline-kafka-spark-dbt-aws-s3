"""
superset_nba_setup.py

This script programmatically creates (or retrieves if already existing) two Trino datasets in Superset:
 - iceberg.iceberg_nba_player_boxscores.scd_player_boxscores
 - iceberg.iceberg_nba_player_boxscores.team_boxscores

Then it creates 50 charts for the 'scd_player_boxscores' dataset.

Usage:
  1. Ensure Superset is running and accessible at the URL/port below.
  2. Adjust the SUPEREST_URL, USERNAME, and PASSWORD if needed.
  3. Run the script in the environment where Superset is reachable.
  4. This script logs in, obtains a CSRF token, lists databases, creates/retrieves datasets,
     and creates 50 charts for the 'scd_player_boxscores' dataset.
"""

import json
import requests
from typing import Any, Dict, List, Optional


class NBASupersetSetup:
    """
    A class to encapsulate methods for:
      1. Authenticating to Superset (with session & CSRF token)
      2. Listing and retrieving the desired database ID
      3. Creating or retrieving a dataset (and defining needed metrics)
      4. Generating multiple charts from that dataset
    """

    def __init__(
        self,
        superset_url: str,
        username: str,
        password: str,
        verify: bool = False
    ) -> None:
        """
        Initialize the NBASupersetSetup instance.

        :param superset_url: The base URL of the Superset instance (e.g. http://localhost:8098)
        :param username: The username for Superset login
        :param password: The password for Superset login
        :param verify: Whether to verify SSL certificates in requests
        """
        self.superset_url = superset_url.rstrip("/")
        self.username = username
        self.password = password
        self.verify = verify

        self.session = requests.Session()
        self.session.verify = verify

        self.access_token: Optional[str] = None
        self.csrf_token: Optional[str] = None

    def login(self) -> None:
        """
        Log in to Superset to obtain a JWT token for subsequent API calls.
        Then retrieve a CSRF token needed for POST/PUT/DELETE calls.
        """
        login_endpoint = f"{self.superset_url}/api/v1/security/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True
        }

        response = self.session.post(login_endpoint, json=payload, timeout=30)
        if response.status_code == 200:
            json_data = response.json()
            self.access_token = json_data["access_token"]
            self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
            print("Login successful!")
        else:
            raise ConnectionError(f"Failed to log in to Superset: {response.text}")

        self._get_csrf_token()

    def _get_csrf_token(self) -> None:
        """
        Retrieve the CSRF token from Superset.
        """
        csrf_endpoint = f"{self.superset_url}/api/v1/security/csrf_token/"
        response = self.session.get(csrf_endpoint, timeout=30)
        if response.status_code == 200:
            data = response.json()
            # The token may appear under different keys depending on Superset version
            if "csrf_token" in data:
                self.csrf_token = data["csrf_token"]
            elif "result" in data:
                val = data["result"]
                if isinstance(val, dict) and "csrf_token" in val:
                    self.csrf_token = val["csrf_token"]
                elif isinstance(val, str):
                    self.csrf_token = val
                else:
                    raise ValueError(f"CSRF token not found in response: {data}")
            else:
                raise ValueError(f"CSRF token not found in response: {data}")

            if not self.csrf_token:
                raise ValueError("CSRF token is empty in response.")

            self.session.headers.update({"X-CSRFToken": self.csrf_token})
            print("CSRF token obtained and added to session headers.")
        else:
            raise ConnectionError(
                f"Failed to fetch CSRF token: {response.status_code}, {response.text}"
            )

    def list_databases(self) -> List[Dict[str, Any]]:
        """
        Fetch and print the id and name of each database known to Superset.
        Returns a list of database records for further use.
        """
        db_endpoint = f"{self.superset_url}/api/v1/database/?q=(page_size:100)"
        response = self.session.get(db_endpoint, timeout=30)

        if response.status_code == 200:
            data = response.json()
            results = data.get("result", [])
            print("Databases found:")
            for db_info in results:
                db_id = db_info["id"]
                db_name = db_info["database_name"]
                print(f"  ID={db_id}, Name={db_name}")
            return results
        else:
            raise ValueError(
                f"Failed to list databases: {response.status_code}, {response.text}"
            )

    def get_database_id_by_name(self, target_name: str) -> int:
        """
        Lists databases and returns the ID of the first database whose name
        matches target_name. Raises ValueError if not found.
        """
        dbs = self.list_databases()
        for db_info in dbs:
            if db_info["database_name"].lower() == target_name.lower():
                return db_info["id"]
        raise ValueError(f"Database '{target_name}' not found in Superset.")

    def find_existing_dataset_id(self, db_id: int, schema: str, table: str) -> Optional[int]:
        """
        Searches for an existing dataset by (db_id, schema, table_name).
        Returns the dataset ID if found, else None.
        """
        ds_endpoint = f"{self.superset_url}/api/v1/dataset/?q=(page:0,page_size:1000)"
        response = self.session.get(ds_endpoint, timeout=30)

        if response.status_code != 200:
            raise ValueError(f"Failed to list datasets: {response.status_code}, {response.text}")

        data = response.json().get("result", [])
        for ds in data:
            if (
                ds["database"]["id"] == db_id
                and ds["schema"] == schema
                and ds["table_name"] == table
            ):
                return ds["id"]

        return None

    def get_or_create_dataset(self, db_id: int, schema: str, table: str) -> int:
        """
        Retrieves the dataset ID if it already exists, otherwise creates it.
        Also ensures the needed aggregator metrics exist so charts referencing
        "sum__...", "avg__...", etc. won't fail.
        Returns the dataset ID.
        """

        # A list of dataset-level metrics that the script references:
        # (sum, avg, max for various columns, plus a custom expression for W/L)
        metrics_definitions = [
            {"metric_name": "sum__points", "expression": "SUM(points)"},
            {"metric_name": "avg__points", "expression": "AVG(points)"},
            {"metric_name": "max__points", "expression": "MAX(points)"},
            {"metric_name": "sum__rebounds", "expression": "SUM(rebounds)"},
            {"metric_name": "max__rebounds", "expression": "MAX(rebounds)"},
            {"metric_name": "sum__blocks", "expression": "SUM(blocks)"},
            {"metric_name": "avg__blocks", "expression": "AVG(blocks)"},
            {"metric_name": "sum__steals", "expression": "SUM(steals)"},
            {"metric_name": "avg__steals", "expression": "AVG(steals)"},
            {"metric_name": "sum__assists", "expression": "SUM(assists)"},
            {"metric_name": "avg__assists", "expression": "AVG(assists)"},
            {"metric_name": "sum__free_throws_attempted", "expression": "SUM(free_throws_attempted)"},
            {"metric_name": "sum__three_point_field_goals_attempted", "expression": "SUM(three_point_field_goals_attempted)"},
            {"metric_name": "sum__turnovers", "expression": "SUM(turnovers)"},
            {"metric_name": "sum__personal_fouls", "expression": "SUM(personal_fouls)"},
            {"metric_name": "avg__personal_fouls", "expression": "AVG(personal_fouls)"},
            {"metric_name": "avg__plus_minus", "expression": "AVG(plus_minus)"},
            {"metric_name": "count", "expression": "COUNT(*)"},
            # Custom expression for counting wins:
            {"metric_name": "COUNT_IF(win_loss = 'W')", "expression": "SUM(CASE WHEN win_loss = 'W' THEN 1 ELSE 0 END)"},
        ]

        existing_id = self.find_existing_dataset_id(db_id, schema, table)
        if existing_id is not None:
            print(f"Dataset {schema}.{table} already exists (ID={existing_id}).")
            # Attempt to update it to include our needed metrics:
            update_url = f"{self.superset_url}/api/v1/dataset/{existing_id}"
            # GET existing dataset, then update metrics
            existing_ds_resp = self.session.get(update_url, timeout=30)
            if existing_ds_resp.status_code != 200:
                raise ValueError(
                    f"Failed to fetch dataset {existing_id} before update: "
                    f"{existing_ds_resp.status_code}, {existing_ds_resp.text}"
                )

            ds_json = existing_ds_resp.json()["result"]
            current_metrics = ds_json.get("metrics", [])

            # Merge new metrics if they're missing
            new_metric_names = {m["metric_name"] for m in metrics_definitions}
            for metric in current_metrics:
                name = metric.get("metric_name")
                if name in new_metric_names:
                    new_metric_names.remove(name)

            for m in metrics_definitions:
                if m["metric_name"] in new_metric_names:
                    current_metrics.append({
                        "metric_name": m["metric_name"],
                        "expression": m["expression"],
                    })

            # -- Remove fields that cause validation errors (created_on, changed_on, etc.) --
            sanitized_metrics = []
            for metric in current_metrics:
                # Keep only known/allowed fields for the PUT payload
                allowed_fields = {
                    "metric_name", "expression", "verbose_name", "description",
                    "warning_text", "metric_type", "d3format", "extra", "currency",
                    "id"
                }
                sanitized_metric = {
                    k: v for k, v in metric.items() if k in allowed_fields
                }
                sanitized_metrics.append(sanitized_metric)

            # Now PUT the updated metrics, using database_id instead of database
            ds_update_payload = {
                "table_name": ds_json["table_name"],
                "schema": ds_json["schema"],
                "database_id": ds_json["database"]["id"],  # Was previously "database"
                "metrics": sanitized_metrics,
            }

            put_resp = self.session.put(update_url, json=ds_update_payload, timeout=30)
            if put_resp.status_code not in (200, 201):
                print(f"WARNING: Failed to update dataset metrics: {put_resp.status_code}, {put_resp.text}")
            else:
                print("Updated dataset metrics to include required sums/averages/etc.")

            return existing_id

        # Otherwise, create new dataset (note: use database_id)
        dataset_endpoint = f"{self.superset_url}/api/v1/dataset/"
        payload = {
            "database_id": db_id,
            "schema": schema,
            "table_name": table,
            "sql": None,
            "metrics": [
                {
                    "metric_name": m["metric_name"],
                    "expression": m["expression"]
                }
                for m in metrics_definitions
            ],
        }
        response = self.session.post(dataset_endpoint, json=payload, timeout=30)
        if response.status_code == 201:
            json_data = response.json()
            new_id = json_data["id"]
            print(f"Successfully created dataset {schema}.{table} (ID={new_id}).")
            return new_id
        else:
            raise ValueError(f"Failed to create dataset: {response.status_code}, {response.text}")

    def create_charts(self, dataset_id: int) -> None:
        """
        Create 50 different charts from the specified dataset.
        """
        chart_endpoint = f"{self.superset_url}/api/v1/chart/"
        datasource_uid = f"{dataset_id}__table"

        base_chart_config: Dict[str, Any] = {
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": "{}",
            "query_context": "{}",
        }

        chart_definitions: List[Dict[str, Any]] = [
            {
                "chart_name": "Correlation Points vs. Rebounds (Scatter)",
                "viz_type": "scatter",
                "description": "Correlation between Points and Rebounds",
                "params": {
                    "metrics": ["points"],
                    "columns": ["rebounds"],
                    "groupby": ["player_name"],
                    "row_limit": 5000,
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Correlation Points vs. Assists (Scatter)",
                "viz_type": "scatter",
                "description": "Correlation between Points and Assists",
                "params": {
                    "metrics": ["points"],
                    "columns": ["assists"],
                    "groupby": ["player_name"],
                    "row_limit": 5000,
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Correlation Rebounds vs. Blocks (Scatter)",
                "viz_type": "scatter",
                "description": "Correlation between Rebounds and Blocks",
                "params": {
                    "metrics": ["rebounds"],
                    "columns": ["blocks"],
                    "groupby": ["player_name"],
                    "row_limit": 5000,
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Field Goal Percentage Distribution (Histogram)",
                "viz_type": "histogram",
                "description": "Distribution of Field Goal Percentage",
                "params": {
                    "all_columns_x": "field_goal_percentage",
                    "adhoc_filters": [],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Points Distribution (Histogram)",
                "viz_type": "histogram",
                "description": "Distribution of Points Scored",
                "params": {
                    "all_columns_x": "points",
                    "adhoc_filters": [],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Rebounds Distribution (Histogram)",
                "viz_type": "histogram",
                "description": "Distribution of Rebounds",
                "params": {
                    "all_columns_x": "rebounds",
                    "adhoc_filters": [],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Season Points Evolution (Line Chart)",
                "viz_type": "line",
                "description": "Points Over Time by Season",
                "params": {
                    "metrics": ["sum__points"],
                    "groupby": ["season"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1Y",
                    "adhoc_filters": [],
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Monthly Points Evolution (Line Chart)",
                "viz_type": "line",
                "description": "Points Over Time (Monthly)",
                "params": {
                    "metrics": ["sum__points"],
                    "groupby": ["year", "month"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1M",
                    "adhoc_filters": [],
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Team Ranking by Avg Points (Bar Chart)",
                "viz_type": "bar",
                "description": "Teams Ranked by Average Points",
                "params": {
                    "metrics": ["avg__points"],
                    "groupby": ["team_name"],
                    "order_desc": True,
                    "row_limit": 50,
                    "adhoc_filters": [],
                    "orientation": "vertical",
                },
            },
            {
                "chart_name": "Team Ranking by Total Wins (Table)",
                "viz_type": "table",
                "description": "Ranking Teams by Win Count",
                "params": {
                    "groupby": ["team_name"],
                    "metrics": ["COUNT_IF(win_loss = 'W')"],
                    "orderby": [["COUNT_IF(win_loss = 'W')", False]],
                    "row_limit": 50,
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Heatmap Points vs. FG% vs. Team",
                "viz_type": "heatmap",
                "description": "Heatmap correlating Points and FG% across Teams",
                "params": {
                    "all_columns_x": "points",
                    "all_columns_y": "field_goal_percentage",
                    "adhoc_filters": [],
                    "groupby": ["team_name"],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Heatmap 3PT% vs. FT% vs. Team",
                "viz_type": "heatmap",
                "description": "Heatmap of 3PT% and FT% for each Team",
                "params": {
                    "all_columns_x": "three_point_field_goal_percentage",
                    "all_columns_y": "free_throw_percentage",
                    "groupby": ["team_name"],
                    "row_limit": 5000,
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Line Chart - Points Over Days of Month",
                "viz_type": "line",
                "description": "Points aggregated by day of month",
                "params": {
                    "metrics": ["sum__points"],
                    "groupby": ["day"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1D",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Bar Chart - Blocks by Team",
                "viz_type": "bar",
                "description": "Average Blocks by Team",
                "params": {
                    "metrics": ["avg__blocks"],
                    "groupby": ["team_name"],
                    "order_desc": True,
                    "row_limit": 50,
                    "orientation": "horizontal",
                },
            },
            {
                "chart_name": "Big Number - Total Games",
                "viz_type": "big_number",
                "description": "Total number of games in dataset",
                "params": {
                    "metric": "count",
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Big Number - Average Points",
                "viz_type": "big_number",
                "description": "Average points across all games",
                "params": {
                    "metric": "avg__points",
                    "adhoc_filters": [],
                },
            },
            {
                "chart_name": "Table - Player Highest Points in a Single Game",
                "viz_type": "table",
                "description": "Show top 10 single-game points performances",
                "params": {
                    "groupby": ["player_name", "game_id"],
                    "metrics": ["max__points"],
                    "orderby": [["max__points", False]],
                    "row_limit": 10,
                },
            },
            {
                "chart_name": "Pie Chart - Distribution of Wins vs. Losses",
                "viz_type": "pie",
                "description": "Win/Loss distribution overall",
                "params": {
                    "metrics": ["count"],
                    "groupby": ["win_loss"],
                    "adhoc_filters": [],
                    "row_limit": 2,
                },
            },
            {
                "chart_name": "Box Plot - Points",
                "viz_type": "box_plot",
                "description": "Points distribution box plot",
                "params": {
                    "groupby": ["team_abbreviation"],
                    "metrics": ["points"],  # Uses raw points column
                    "adhoc_filters": [],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Box Plot - Rebounds",
                "viz_type": "box_plot",
                "description": "Rebounds distribution box plot",
                "params": {
                    "groupby": ["team_abbreviation"],
                    "metrics": ["rebounds"],  # Uses raw rebounds column
                    "adhoc_filters": [],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Histogram - Turnovers",
                "viz_type": "histogram",
                "description": "Distribution of Turnovers",
                "params": {
                    "all_columns_x": "turnovers",
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Histogram - Steals",
                "viz_type": "histogram",
                "description": "Distribution of Steals",
                "params": {
                    "all_columns_x": "steals",
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Bar Chart - Average Assists by Team",
                "viz_type": "bar",
                "description": "Ranking teams by average assists",
                "params": {
                    "metrics": ["avg__assists"],
                    "groupby": ["team_name"],
                    "order_desc": True,
                    "orientation": "vertical",
                    "row_limit": 50,
                },
            },
            {
                "chart_name": "Bar Chart - Average Steals by Team",
                "viz_type": "bar",
                "description": "Ranking teams by average steals",
                "params": {
                    "metrics": ["avg__steals"],
                    "groupby": ["team_name"],
                    "order_desc": True,
                    "orientation": "vertical",
                    "row_limit": 50,
                },
            },
            {
                "chart_name": "Line Chart - FT% Evolution Over Months",
                "viz_type": "line",
                "description": "Free throw percentage over months",
                "params": {
                    "metrics": ["avg__free_throw_percentage"],
                    "groupby": ["year", "month"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1M",
                    "adhoc_filters": [],
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Line Chart - 3PT% Evolution Over Years",
                "viz_type": "line",
                "description": "Three-point percentage by year",
                "params": {
                    "metrics": ["avg__three_point_field_goal_percentage"],
                    "groupby": ["season"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1Y",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Bar Chart - Top 10 Players by Total Points",
                "viz_type": "bar",
                "description": "Top 10 players in total points",
                "params": {
                    "metrics": ["sum__points"],
                    "groupby": ["player_name"],
                    "order_desc": True,
                    "row_limit": 10,
                },
            },
            {
                "chart_name": "Bar Chart - Top 10 Players by Total Rebounds",
                "viz_type": "bar",
                "description": "Top 10 players in total rebounds",
                "params": {
                    "metrics": ["sum__rebounds"],
                    "groupby": ["player_name"],
                    "order_desc": True,
                    "row_limit": 10,
                },
            },
            {
                "chart_name": "Bar Chart - Top 10 Players by Total Blocks",
                "viz_type": "bar",
                "description": "Top 10 players in total blocks",
                "params": {
                    "metrics": ["sum__blocks"],
                    "groupby": ["player_name"],
                    "order_desc": True,
                    "row_limit": 10,
                },
            },
            {
                "chart_name": "Bar Chart - Top 10 Players by Total Steals",
                "viz_type": "bar",
                "description": "Top 10 players in total steals",
                "params": {
                    "metrics": ["sum__steals"],
                    "groupby": ["player_name"],
                    "order_desc": True,
                    "row_limit": 10,
                },
            },
            {
                "chart_name": "Heatmap Points vs. 3PTM vs. Team",
                "viz_type": "heatmap",
                "description": "Heatmap of points vs. 3PT made by team",
                "params": {
                    "all_columns_x": "points",
                    "all_columns_y": "three_point_field_goals_made",
                    "groupby": ["team_name"],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Heatmap Points vs. FT Made vs. Team",
                "viz_type": "heatmap",
                "description": "Heatmap of points vs. free throws made by team",
                "params": {
                    "all_columns_x": "points",
                    "all_columns_y": "free_throws_made",
                    "groupby": ["team_name"],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Big Number - Max Points by Any Player",
                "viz_type": "big_number",
                "description": "Maximum single-game points by any player",
                "params": {
                    "metric": "max__points",
                },
            },
            {
                "chart_name": "Big Number - Max Rebounds by Any Player",
                "viz_type": "big_number",
                "description": "Maximum single-game rebounds by any player",
                "params": {
                    "metric": "max__rebounds",
                },
            },
            {
                "chart_name": "Line Chart - Evolution of Average Plus/Minus",
                "viz_type": "line",
                "description": "Average plus_minus over months",
                "params": {
                    "metrics": ["avg__plus_minus"],
                    "groupby": ["year", "month"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1M",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Table - Season and Win/Loss Count",
                "viz_type": "table",
                "description": "Count wins/losses by season",
                "params": {
                    "groupby": ["season", "win_loss"],
                    "metrics": ["count"],
                    "orderby": [["count", False]],
                    "row_limit": 100,
                },
            },
            {
                "chart_name": "Line Chart - Evolution of Assists by Season",
                "viz_type": "line",
                "description": "Total assists across each season",
                "params": {
                    "metrics": ["sum__assists"],
                    "groupby": ["season"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1Y",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Line Chart - Evolution of Steals by Season",
                "viz_type": "line",
                "description": "Total steals across each season",
                "params": {
                    "metrics": ["sum__steals"],
                    "groupby": ["season"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1Y",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Line Chart - Evolution of Blocks by Season",
                "viz_type": "line",
                "description": "Total blocks across each season",
                "params": {
                    "metrics": ["sum__blocks"],
                    "groupby": ["season"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1Y",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Line Chart - Evolution of Turnovers by Season",
                "viz_type": "line",
                "description": "Total turnovers across each season",
                "params": {
                    "metrics": ["sum__turnovers"],
                    "groupby": ["season"],
                    "granularity_sqla": "game_date",
                    "time_grain_sqla": "P1Y",
                    "time_range": "No filter",
                },
            },
            {
                "chart_name": "Pie Chart - Home vs. Away Games",
                "viz_type": "pie",
                "description": "Count of home games vs. away games",
                "params": {
                    "groupby": ["home_team", "away_team"],
                    "metrics": ["count"],
                    "row_limit": 100,
                },
            },
            {
                "chart_name": "Pie Chart - Fouls Distribution",
                "viz_type": "pie",
                "description": "Total personal fouls distribution by team",
                "params": {
                    "metrics": ["sum__personal_fouls"],
                    "groupby": ["team_name"],
                    "row_limit": 100,
                },
            },
            {
                "chart_name": "Bar Chart - Average Fouls by Player",
                "viz_type": "bar",
                "description": "Ranking players by average personal fouls",
                "params": {
                    "metrics": ["avg__personal_fouls"],
                    "groupby": ["player_name"],
                    "order_desc": True,
                    "row_limit": 20,
                },
            },
            {
                "chart_name": "Table - Points and Rebounds by Player",
                "viz_type": "table",
                "description": "Listing points and rebounds by player",
                "params": {
                    "groupby": ["player_name"],
                    "metrics": ["sum__points", "sum__rebounds"],
                    "orderby": [["sum__points", False]],
                    "row_limit": 50,
                },
            },
            {
                "chart_name": "Table - Assists and Steals by Player",
                "viz_type": "table",
                "description": "Listing assists and steals by player",
                "params": {
                    "groupby": ["player_name"],
                    "metrics": ["sum__assists", "sum__steals"],
                    "orderby": [["sum__assists", False]],
                    "row_limit": 50,
                },
            },
            {
                "chart_name": "Histogram - Plus Minus",
                "viz_type": "histogram",
                "description": "Distribution of plus_minus",
                "params": {
                    "all_columns_x": "plus_minus",
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Box Plot - Field Goal Percentage",
                "viz_type": "box_plot",
                "description": "Box plot of FG% grouped by team_abbreviation",
                "params": {
                    "groupby": ["team_abbreviation"],
                    "metrics": ["field_goal_percentage"],
                    "adhoc_filters": [],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Histogram - Fantasy Points",
                "viz_type": "histogram",
                "description": "Distribution of fantasy_points",
                "params": {
                    "all_columns_x": "fantasy_points",
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Bar Chart - Total 3PT Attempted by Season",
                "viz_type": "bar",
                "description": "Seasonal sum of 3PT attempts",
                "params": {
                    "metrics": ["sum__three_point_field_goals_attempted"],
                    "groupby": ["season"],
                    "row_limit": 50,
                    "order_desc": True,
                },
            },
            {
                "chart_name": "Bar Chart - Total FT Attempted by Season",
                "viz_type": "bar",
                "description": "Seasonal sum of free throw attempts",
                "params": {
                    "metrics": ["sum__free_throws_attempted"],
                    "groupby": ["season"],
                    "row_limit": 50,
                    "order_desc": True,
                },
            },
        ]

        for i, chart_def in enumerate(chart_definitions, start=1):
            chart_payload = {
                **base_chart_config,
                "slice_name": chart_def["chart_name"],
                "viz_type": chart_def["viz_type"],
                "description": chart_def["description"],
                "params": json.dumps(chart_def["params"]),
                "datasource_id": dataset_id,
                "datasource_name": datasource_uid,
            }

            response = self.session.post(chart_endpoint, json=chart_payload, timeout=30)
            if response.status_code == 201:
                chart_id = response.json().get("id", "UNKNOWN")
                print(f"[{i}/50] Chart created: '{chart_def['chart_name']}' (ID={chart_id})")
            else:
                print(
                    f"[{i}/50] Failed to create chart '{chart_def['chart_name']}': "
                    f"{response.status_code}, {response.text}"
                )

    def run_setup(self) -> None:
        """
        High-level workflow to:
          1) Login (obtain JWT + CSRF token)
          2) List databases & retrieve the correct database ID
          3) Create or retrieve each dataset in a loop BEFORE creating any charts
          4) Create 50 charts for the 'scd_player_boxscores' dataset
        """
        self.login()
        db_id = self.get_database_id_by_name("Trino")

        datasets_to_create = {
            "scd_player_boxscores": {
                "schema": "iceberg_nba_player_boxscores",
                "table_name": "scd_player_boxscores"
            },
            "team_boxscores": {
                "schema": "iceberg_nba_player_boxscores",
                "table_name": "team_boxscores"
            },
        }

        dataset_ids: Dict[str, int] = {}
        for ds_key, ds_info in datasets_to_create.items():
            ds_id = self.get_or_create_dataset(
                db_id=db_id,
                schema=ds_info["schema"],
                table=ds_info["table_name"]
            )
            dataset_ids[ds_key] = ds_id

        scd_dataset_id = dataset_ids["scd_player_boxscores"]
        self.create_charts(scd_dataset_id)

        print("\nDatasets created or retrieved:")
        for k, v in dataset_ids.items():
            print(f"  {k} => dataset_id={v}")


def main() -> None:
    """
    Main entry point to run the NBASupersetSetup.
    Adjust SUPERSET_URL, USERNAME, PASSWORD as needed.
    """
    SUPERSET_URL = "http://localhost:8098"
    USERNAME = "superset"
    PASSWORD = "Password1234!"

    setup_obj = NBASupersetSetup(
        superset_url=SUPERSET_URL,
        username=USERNAME,
        password=PASSWORD
    )
    setup_obj.run_setup()


if __name__ == "__main__":
    main()
