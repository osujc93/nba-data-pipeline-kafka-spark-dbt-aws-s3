#!/usr/bin/env python3
"""
logs in, obtains a CSRF token, lists databases, creates/retrieves datasets,
    and creates the charts for both datasets.
"""

import json
import requests
from typing import Any, Dict, List, Optional


class NBASupersetSetup:
    def __init__(
        self,
        superset_url: str,
        username: str,
        password: str,
        verify: bool = False
    ) -> None:
        """
        Initialize the NBASupersetSetup instance.

        :param superset_url: The base URL of the Superset instance (e.g. http://superset:8098)
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

    def get_player_metrics_definitions(self) -> List[Dict[str, Any]]:
        """
        Returns a list of aggregator metrics definitions for the 'scd_player_boxscores' table.
        """
        return [
            # Custom metric:
            {
                "metric_name": "COUNT_IF(win_loss = 'W')",
                "expression": "SUM(CASE WHEN win_loss = 'W' THEN 1 ELSE 0 END)"
            },

            # minutes_played (int)
            {"metric_name": "avg__minutes_played", "expression": "AVG(minutes_played)"},
            {"metric_name": "max__minutes_played", "expression": "MAX(minutes_played)"},
            {"metric_name": "min__minutes_played", "expression": "MIN(minutes_played)"},
            {"metric_name": "sum__minutes_played", "expression": "SUM(minutes_played)"},

            # field_goals_made (int)
            {"metric_name": "avg__field_goals_made", "expression": "AVG(field_goals_made)"},
            {"metric_name": "count__field_goals_made", "expression": "COUNT(field_goals_made)"},
            {"metric_name": "count_distinct__field_goals_made", "expression": "COUNT(DISTINCT field_goals_made)"},
            {"metric_name": "max__field_goals_made", "expression": "MAX(field_goals_made)"},
            {"metric_name": "min__field_goals_made", "expression": "MIN(field_goals_made)"},
            {"metric_name": "sum__field_goals_made", "expression": "SUM(field_goals_made)"},

            # field_goals_attempted (int)
            {"metric_name": "avg__field_goals_attempted", "expression": "AVG(field_goals_attempted)"},
            {"metric_name": "count__field_goals_attempted", "expression": "COUNT(field_goals_attempted)"},
            {"metric_name": "count_distinct__field_goals_attempted", "expression": "COUNT(DISTINCT field_goals_attempted)"},
            {"metric_name": "max__field_goals_attempted", "expression": "MAX(field_goals_attempted)"},
            {"metric_name": "min__field_goals_attempted", "expression": "MIN(field_goals_attempted)"},
            {"metric_name": "sum__field_goals_attempted", "expression": "SUM(field_goals_attempted)"},

            # field_goal_percentage (double)
            {"metric_name": "avg__field_goal_percentage", "expression": "AVG(field_goal_percentage)"},
            {"metric_name": "count__field_goal_percentage", "expression": "COUNT(field_goal_percentage)"},
            {"metric_name": "count_distinct__field_goal_percentage", "expression": "COUNT(DISTINCT field_goal_percentage)"},
            {"metric_name": "max__field_goal_percentage", "expression": "MAX(field_goal_percentage)"},
            {"metric_name": "min__field_goal_percentage", "expression": "MIN(field_goal_percentage)"},
            {"metric_name": "sum__field_goal_percentage", "expression": "SUM(field_goal_percentage)"},

            # three_point_field_goals_made (int)
            {"metric_name": "avg__three_point_field_goals_made", "expression": "AVG(three_point_field_goals_made)"},
            {"metric_name": "count__three_point_field_goals_made", "expression": "COUNT(three_point_field_goals_made)"},
            {"metric_name": "count_distinct__three_point_field_goals_made", "expression": "COUNT(DISTINCT three_point_field_goals_made)"},
            {"metric_name": "max__three_point_field_goals_made", "expression": "MAX(three_point_field_goals_made)"},
            {"metric_name": "min__three_point_field_goals_made", "expression": "MIN(three_point_field_goals_made)"},
            {"metric_name": "sum__three_point_field_goals_made", "expression": "SUM(three_point_field_goals_made)"},

            # three_point_field_goals_attempted (int)
            {"metric_name": "avg__three_point_field_goals_attempted", "expression": "AVG(three_point_field_goals_attempted)"},
            {"metric_name": "count__three_point_field_goals_attempted", "expression": "COUNT(three_point_field_goals_attempted)"},
            {"metric_name": "count_distinct__three_point_field_goals_attempted", "expression": "COUNT(DISTINCT three_point_field_goals_attempted)"},
            {"metric_name": "max__three_point_field_goals_attempted", "expression": "MAX(three_point_field_goals_attempted)"},
            {"metric_name": "min__three_point_field_goals_attempted", "expression": "MIN(three_point_field_goals_attempted)"},
            {"metric_name": "sum__three_point_field_goals_attempted", "expression": "SUM(three_point_field_goals_attempted)"},

            # three_point_field_goal_percentage (double)
            {"metric_name": "avg__three_point_field_goal_percentage", "expression": "AVG(three_point_field_goal_percentage)"},
            {"metric_name": "count__three_point_field_goal_percentage", "expression": "COUNT(three_point_field_goal_percentage)"},
            {"metric_name": "count_distinct__three_point_field_goal_percentage", "expression": "COUNT(DISTINCT three_point_field_goal_percentage)"},
            {"metric_name": "max__three_point_field_goal_percentage", "expression": "MAX(three_point_field_goal_percentage)"},
            {"metric_name": "min__three_point_field_goal_percentage", "expression": "MIN(three_point_field_goal_percentage)"},
            {"metric_name": "sum__three_point_field_goal_percentage", "expression": "SUM(three_point_field_goal_percentage)"},

            # free_throws_made (int)
            {"metric_name": "avg__free_throws_made", "expression": "AVG(free_throws_made)"},
            {"metric_name": "count__free_throws_made", "expression": "COUNT(free_throws_made)"},
            {"metric_name": "count_distinct__free_throws_made", "expression": "COUNT(DISTINCT free_throws_made)"},
            {"metric_name": "max__free_throws_made", "expression": "MAX(free_throws_made)"},
            {"metric_name": "min__free_throws_made", "expression": "MIN(free_throws_made)"},
            {"metric_name": "sum__free_throws_made", "expression": "SUM(free_throws_made)"},

            # free_throws_attempted (int)
            {"metric_name": "avg__free_throws_attempted", "expression": "AVG(free_throws_attempted)"},
            {"metric_name": "count__free_throws_attempted", "expression": "COUNT(free_throws_attempted)"},
            {"metric_name": "count_distinct__free_throws_attempted", "expression": "COUNT(DISTINCT free_throws_attempted)"},
            {"metric_name": "max__free_throws_attempted", "expression": "MAX(free_throws_attempted)"},
            {"metric_name": "min__free_throws_attempted", "expression": "MIN(free_throws_attempted)"},
            {"metric_name": "sum__free_throws_attempted", "expression": "SUM(free_throws_attempted)"},

            # free_throw_percentage (double)
            {"metric_name": "avg__free_throw_percentage", "expression": "AVG(free_throw_percentage)"},
            {"metric_name": "count__free_throw_percentage", "expression": "COUNT(free_throw_percentage)"},
            {"metric_name": "count_distinct__free_throw_percentage", "expression": "COUNT(DISTINCT free_throw_percentage)"},
            {"metric_name": "max__free_throw_percentage", "expression": "MAX(free_throw_percentage)"},
            {"metric_name": "min__free_throw_percentage", "expression": "MIN(free_throw_percentage)"},
            {"metric_name": "sum__free_throw_percentage", "expression": "SUM(free_throw_percentage)"},

            # offensive_rebounds (int)
            {"metric_name": "avg__offensive_rebounds", "expression": "AVG(offensive_rebounds)"},
            {"metric_name": "count__offensive_rebounds", "expression": "COUNT(offensive_rebounds)"},
            {"metric_name": "count_distinct__offensive_rebounds", "expression": "COUNT(DISTINCT offensive_rebounds)"},
            {"metric_name": "max__offensive_rebounds", "expression": "MAX(offensive_rebounds)"},
            {"metric_name": "min__offensive_rebounds", "expression": "MIN(offensive_rebounds)"},
            {"metric_name": "sum__offensive_rebounds", "expression": "SUM(offensive_rebounds)"},

            # defensive_rebounds (int)
            {"metric_name": "avg__defensive_rebounds", "expression": "AVG(defensive_rebounds)"},
            {"metric_name": "count__defensive_rebounds", "expression": "COUNT(defensive_rebounds)"},
            {"metric_name": "count_distinct__defensive_rebounds", "expression": "COUNT(DISTINCT defensive_rebounds)"},
            {"metric_name": "max__defensive_rebounds", "expression": "MAX(defensive_rebounds)"},
            {"metric_name": "min__defensive_rebounds", "expression": "MIN(defensive_rebounds)"},
            {"metric_name": "sum__defensive_rebounds", "expression": "SUM(defensive_rebounds)"},

            # rebounds (int)
            {"metric_name": "avg__rebounds", "expression": "AVG(rebounds)"},
            {"metric_name": "count__rebounds", "expression": "COUNT(rebounds)"},
            {"metric_name": "count_distinct__rebounds", "expression": "COUNT(DISTINCT rebounds)"},
            {"metric_name": "max__rebounds", "expression": "MAX(rebounds)"},
            {"metric_name": "min__rebounds", "expression": "MIN(rebounds)"},
            {"metric_name": "sum__rebounds", "expression": "SUM(rebounds)"},

            # assists (int)
            {"metric_name": "avg__assists", "expression": "AVG(assists)"},
            {"metric_name": "count__assists", "expression": "COUNT(assists)"},
            {"metric_name": "count_distinct__assists", "expression": "COUNT(DISTINCT assists)"},
            {"metric_name": "max__assists", "expression": "MAX(assists)"},
            {"metric_name": "min__assists", "expression": "MIN(assists)"},
            {"metric_name": "sum__assists", "expression": "SUM(assists)"},

            # steals (int)
            {"metric_name": "avg__steals", "expression": "AVG(steals)"},
            {"metric_name": "count__steals", "expression": "COUNT(steals)"},
            {"metric_name": "count_distinct__steals", "expression": "COUNT(DISTINCT steals)"},
            {"metric_name": "max__steals", "expression": "MAX(steals)"},
            {"metric_name": "min__steals", "expression": "MIN(steals)"},
            {"metric_name": "sum__steals", "expression": "SUM(steals)"},

            # blocks (int)
            {"metric_name": "avg__blocks", "expression": "AVG(blocks)"},
            {"metric_name": "count__blocks", "expression": "COUNT(blocks)"},
            {"metric_name": "count_distinct__blocks", "expression": "COUNT(DISTINCT blocks)"},
            {"metric_name": "max__blocks", "expression": "MAX(blocks)"},
            {"metric_name": "min__blocks", "expression": "MIN(blocks)"},
            {"metric_name": "sum__blocks", "expression": "SUM(blocks)"},

            # turnovers (int)
            {"metric_name": "avg__turnovers", "expression": "AVG(turnovers)"},
            {"metric_name": "count__turnovers", "expression": "COUNT(turnovers)"},
            {"metric_name": "count_distinct__turnovers", "expression": "COUNT(DISTINCT turnovers)"},
            {"metric_name": "max__turnovers", "expression": "MAX(turnovers)"},
            {"metric_name": "min__turnovers", "expression": "MIN(turnovers)"},
            {"metric_name": "sum__turnovers", "expression": "SUM(turnovers)"},

            # personal_fouls (int)
            {"metric_name": "avg__personal_fouls", "expression": "AVG(personal_fouls)"},
            {"metric_name": "count__personal_fouls", "expression": "COUNT(personal_fouls)"},
            {"metric_name": "count_distinct__personal_fouls", "expression": "COUNT(DISTINCT personal_fouls)"},
            {"metric_name": "max__personal_fouls", "expression": "MAX(personal_fouls)"},
            {"metric_name": "min__personal_fouls", "expression": "MIN(personal_fouls)"},
            {"metric_name": "sum__personal_fouls", "expression": "SUM(personal_fouls)"},

            # points (int)
            {"metric_name": "avg__points", "expression": "AVG(points)"},
            {"metric_name": "count__points", "expression": "COUNT(points)"},
            {"metric_name": "count_distinct__points", "expression": "COUNT(DISTINCT points)"},
            {"metric_name": "max__points", "expression": "MAX(points)"},
            {"metric_name": "min__points", "expression": "MIN(points)"},
            {"metric_name": "sum__points", "expression": "SUM(points)"},

            # year (int)
            {"metric_name": "count__year", "expression": "COUNT(year)"},
            {"metric_name": "count_distinct__year", "expression": "COUNT(DISTINCT year)"},

            # month (int)
            {"metric_name": "count__month", "expression": "COUNT(month)"},
            {"metric_name": "count_distinct__month", "expression": "COUNT(DISTINCT month)"},

            # day (int)
            {"metric_name": "count__day", "expression": "COUNT(day)"},
            {"metric_name": "count_distinct__day", "expression": "COUNT(DISTINCT day)"},

            # season_id
            {"metric_name": "count__season_id", "expression": "COUNT(season_id)"},
            {"metric_name": "count_distinct__season_id", "expression": "COUNT(DISTINCT season_id)"},

            # player_name
            {"metric_name": "count__player_name", "expression": "COUNT(player_name)"},
            {"metric_name": "count_distinct__player_name", "expression": "COUNT(DISTINCT player_name)"},

            # team_name
            {"metric_name": "count__team_name", "expression": "COUNT(team_name)"},
            {"metric_name": "count_distinct__team_name", "expression": "COUNT(DISTINCT team_name)"},

            # game_id
            {"metric_name": "count__game_id", "expression": "COUNT(game_id)"},
            {"metric_name": "count_distinct__game_id", "expression": "COUNT(DISTINCT game_id)"},

            # matchup
            {"metric_name": "count__matchup", "expression": "COUNT(matchup)"},
            {"metric_name": "count_distinct__matchup", "expression": "COUNT(DISTINCT matchup)"},

            # home_team
            {"metric_name": "count__home_team", "expression": "COUNT(home_team)"},
            {"metric_name": "count_distinct__home_team", "expression": "COUNT(DISTINCT home_team)"},

            # away_team
            {"metric_name": "count__away_team", "expression": "COUNT(away_team)"},
            {"metric_name": "count_distinct__away_team", "expression": "COUNT(DISTINCT away_team)"},

            {
                "metric_name": "total_triple_doubles",
                "expression": """
                    SUM(
                        CASE WHEN (
                            (CASE WHEN assists >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN blocks >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN points >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN rebounds >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN steals >= 10 THEN 1 ELSE 0 END)
                        ) >= 3
                        THEN 1 ELSE 0 END
                    )
                """.strip()
            },
            {
                "metric_name": "total_double_doubles",
                "expression": """
                    SUM(
                        CASE WHEN (
                            (CASE WHEN points >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN rebounds >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN assists >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN steals >= 10 THEN 1 ELSE 0 END) +
                            (CASE WHEN blocks >= 10 THEN 1 ELSE 0 END)
                        ) >= 2
                        THEN 1 ELSE 0 END
                    )
                """.strip()
            },
        ]

    def get_team_metrics_definitions(self) -> List[Dict[str, Any]]:
        """
        Returns a list of aggregator metrics definitions for the 'scd_team_boxscores' table.
        """
        return [
            # Team Win metric
            {
                "metric_name": "COUNT_IF(win_loss = 'W')",
                "expression": "SUM(CASE WHEN win_loss = 'W' THEN 1 ELSE 0 END)"
            },

            # Basic aggregator metrics for relevant columns:
            # team_points (bigint)
            {"metric_name": "avg__team_points", "expression": "AVG(team_points)"},
            {"metric_name": "sum__team_points", "expression": "SUM(team_points)"},
            {"metric_name": "max__team_points", "expression": "MAX(team_points)"},
            {"metric_name": "min__team_points", "expression": "MIN(team_points)"},

            # team_fga (bigint)
            {"metric_name": "avg__team_fga", "expression": "AVG(team_fga)"},
            {"metric_name": "sum__team_fga", "expression": "SUM(team_fga)"},

            # team_fgm (bigint)
            {"metric_name": "avg__team_fgm", "expression": "AVG(team_fgm)"},
            {"metric_name": "sum__team_fgm", "expression": "SUM(team_fgm)"},

            # team_fg3a (bigint)
            {"metric_name": "avg__team_fg3a", "expression": "AVG(team_fg3a)"},
            {"metric_name": "sum__team_fg3a", "expression": "SUM(team_fg3a)"},

            # team_fg3m (bigint)
            {"metric_name": "avg__team_fg3m", "expression": "AVG(team_fg3m)"},
            {"metric_name": "sum__team_fg3m", "expression": "SUM(team_fg3m)"},

            # team_fta (bigint)
            {"metric_name": "avg__team_fta", "expression": "AVG(team_fta)"},
            {"metric_name": "sum__team_fta", "expression": "SUM(team_fta)"},

            # team_ftm (bigint)
            {"metric_name": "avg__team_ftm", "expression": "AVG(team_ftm)"},
            {"metric_name": "sum__team_ftm", "expression": "SUM(team_ftm)"},

            # team_tov (bigint)
            {"metric_name": "avg__team_tov", "expression": "AVG(team_tov)"},
            {"metric_name": "sum__team_tov", "expression": "SUM(team_tov)"},

            # team_oreb (bigint)
            {"metric_name": "avg__team_oreb", "expression": "AVG(team_oreb)"},
            {"metric_name": "sum__team_oreb", "expression": "SUM(team_oreb)"},

            # team_dreb (bigint)
            {"metric_name": "avg__team_dreb", "expression": "AVG(team_dreb)"},
            {"metric_name": "sum__team_dreb", "expression": "SUM(team_dreb)"},

            # team_reb (bigint)
            {"metric_name": "avg__team_reb", "expression": "AVG(team_reb)"},
            {"metric_name": "sum__team_reb", "expression": "SUM(team_reb)"},

            # team_ast (bigint)
            {"metric_name": "avg__team_ast", "expression": "AVG(team_ast)"},
            {"metric_name": "sum__team_ast", "expression": "SUM(team_ast)"},

            # team_stl (bigint)
            {"metric_name": "avg__team_stl", "expression": "AVG(team_stl)"},
            {"metric_name": "sum__team_stl", "expression": "SUM(team_stl)"},

            # team_blk (bigint)
            {"metric_name": "avg__team_blk", "expression": "AVG(team_blk)"},
            {"metric_name": "sum__team_blk", "expression": "SUM(team_blk)"},

            # partial_possessions (decimal)
            {"metric_name": "sum__partial_possessions", "expression": "SUM(partial_possessions)"},
            {"metric_name": "avg__partial_possessions", "expression": "AVG(partial_possessions)"},

            # team_fg_pct (decimal)
            {"metric_name": "avg__team_fg_pct", "expression": "AVG(team_fg_pct)"},
            {"metric_name": "max__team_fg_pct", "expression": "MAX(team_fg_pct)"},
            {"metric_name": "min__team_fg_pct", "expression": "MIN(team_fg_pct)"},

            # team_fg3_pct (decimal)
            {"metric_name": "avg__team_fg3_pct", "expression": "AVG(team_fg3_pct)"},
            {"metric_name": "max__team_fg3_pct", "expression": "MAX(team_fg3_pct)"},
            {"metric_name": "min__team_fg3_pct", "expression": "MIN(team_fg3_pct)"},

            # team_ft_pct (decimal)
            {"metric_name": "avg__team_ft_pct", "expression": "AVG(team_ft_pct)"},
            {"metric_name": "max__team_ft_pct", "expression": "MAX(team_ft_pct)"},
            {"metric_name": "min__team_ft_pct", "expression": "MIN(team_ft_pct)"},
        ]

    def get_or_create_dataset(
        self,
        db_id: int,
        schema: str,
        table: str,
        metrics: List[Dict[str, Any]]
    ) -> int:
        """
        Retrieves the dataset ID if it already exists, otherwise creates it,
        then updates it to ensure the needed aggregator metrics exist.

        :param db_id: The database ID in Superset
        :param schema: The schema name
        :param table: The table name
        :param metrics: The aggregator metrics definitions for that table
        :return: The dataset ID
        """
        existing_id = self.find_existing_dataset_id(db_id, schema, table)
        if existing_id is not None:
            print(f"Dataset {schema}.{table} already exists (ID={existing_id}).")
            return self._ensure_dataset_metrics(existing_id, metrics)
        else:
            dataset_endpoint = f"{self.superset_url}/api/v1/dataset/"
            create_payload = {
                "database": db_id,
                "schema": schema,
                "table_name": table,
            }
            response = self.session.post(dataset_endpoint, json=create_payload, timeout=30)
            if response.status_code == 201:
                json_data = response.json()
                new_id = json_data["id"]
                print(f"Successfully created dataset {schema}.{table} (ID={new_id}).")
                return self._ensure_dataset_metrics(new_id, metrics)
            else:
                raise ValueError(f"Failed to create dataset: {response.status_code}, {response.text}")

    def _ensure_dataset_metrics(self, dataset_id: int, metrics: List[Dict[str, Any]]) -> int:
        """
        Given an existing dataset ID, fetch it, merge in needed metrics, and PUT the updated dataset.
        """
        update_url = f"{self.superset_url}/api/v1/dataset/{dataset_id}"
        existing_ds_resp = self.session.get(update_url, timeout=30)
        if existing_ds_resp.status_code != 200:
            raise ValueError(
                f"Failed to fetch dataset {dataset_id} before update: "
                f"{existing_ds_resp.status_code}, {existing_ds_resp.text}"
            )

        ds_json = existing_ds_resp.json()["result"]
        current_metrics = ds_json.get("metrics", [])

        # Determine which metric names are missing
        new_metric_names = {m["metric_name"] for m in metrics}
        for metric in current_metrics:
            name = metric.get("metric_name")
            if name in new_metric_names:
                new_metric_names.remove(name)

        # Append only the missing metrics
        for m in metrics:
            if m["metric_name"] in new_metric_names:
                current_metrics.append({
                    "metric_name": m["metric_name"],
                    "expression": m["expression"],
                })

        # Remove fields that cause validation errors
        allowed_fields = {
            "metric_name", "expression", "verbose_name", "description",
            "warning_text", "metric_type", "d3format", "extra", "currency", "id"
        }
        sanitized_metrics = [
            {k: v for k, v in metric.items() if k in allowed_fields}
            for metric in current_metrics
        ]

        ds_update_payload = {
            "table_name": ds_json["table_name"],
            "schema": ds_json["schema"],
            "database_id": ds_json["database"]["id"],
            "metrics": sanitized_metrics,
        }

        put_resp = self.session.put(update_url, json=ds_update_payload, timeout=30)
        if put_resp.status_code not in (200, 201):
            print(f"WARNING: Failed to update dataset metrics: {put_resp.status_code}, {put_resp.text}")
        else:
            print("Updated dataset metrics to include required aggregations.")

        return dataset_id

    def create_player_charts(self, dataset_id: int) -> None:
        """
        Create charts from the player dataset (scd_player_boxscores).
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
                "chart_name": "Top 5 Single-Game Points Overall",
                "viz_type": "table",
                "description": "Shows top 5 single-game point performances across all players",
                "params": {
                    "metrics": ["max__points"],
                    "groupby": ["player_id", "player_name", "season", "game_id", "game_date"],
                    "orderby": [["max__points", False]],
                    "row_limit": 5
                },
            },
            {
                "chart_name": "Average Player Stats Per Season",
                "viz_type": "table",
                "description": "AVG points, rebounds, assists, FG%, and count distinct games",
                "params": {
                    "metrics": [
                        "avg__points",
                        "avg__rebounds",
                        "avg__assists",
                        "avg__field_goal_percentage",
                        "count_distinct__game_id"
                    ],
                    "groupby": ["player_id", "player_name", "season"],
                    "orderby": [["avg__points", False]],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Most Career Triple/Doubles",
                "viz_type": "table",
                "description": "Most Career Triple/Doubles",
                "params": {
                    "groupby": ["player_name"],
                    "metrics": ["total_triple_doubles"],
                    "orderby": [["total_triple_doubles", False]],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "PER SEASON TYPE Most Triple/Doubles",
                "viz_type": "table",
                "description": "Most Triple/Doubles per season type",
                "params": {
                    "groupby": ["player_name", "season", "season_type"],
                    "metrics": ["total_triple_doubles"],
                    "orderby": [["season", False]],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Most Career Double/Doubles",
                "viz_type": "table",
                "description": "Most Career Double/Doubles",
                "params": {
                    "groupby": ["player_name"],
                    "metrics": ["total_double_doubles"],
                    "orderby": [["total_double_doubles", False]],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "PER SEASON TYPE Most Double/Doubles",
                "viz_type": "table",
                "description": "Most Double/Doubles per season type",
                "params": {
                    "groupby": ["player_name", "season", "season_type"],
                    "metrics": ["total_double_doubles"],
                    "orderby": [["season", False]],
                    "row_limit": 5000,
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
                print(f"[{i}/{len(chart_definitions)}] Chart created: '{chart_def['chart_name']}' (ID={chart_id})")
            else:
                print(
                    f"[{i}/{len(chart_definitions)}] Failed to create chart '{chart_def['chart_name']}': "
                    f"{response.status_code}, {response.text}"
                )

    def create_team_charts(self, dataset_id: int) -> None:
        """
        Create charts from the team dataset (scd_team_boxscores).
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
                "chart_name": "Teams Avg Points Per Game (Aggregator)",
                "viz_type": "table",
                "description": "Teams' average points per game by season (scd_team_boxscores)",
                "params": {
                    "groupby": ["season", "team_id", "team_name"],
                    "metrics": ["avg__team_points"],
                    "orderby": [["avg__team_points", False]],
                    "row_limit": 5000,
                },
            },
            {
                "chart_name": "Top 5 Single-Game Team Points",
                "viz_type": "table",
                "description": "Shows top 5 single-game point performances by a team",
                "params": {
                    "metrics": ["max__team_points"],
                    "groupby": ["team_id", "team_name", "season", "game_id", "game_date"],
                    "orderby": [["max__team_points", False]],
                    "row_limit": 5
                },
            },
            {
                "chart_name": "Team Win Count (Aggregator)",
                "viz_type": "table",
                "description": "Count of how many wins a given team has in the data",
                "params": {
                    "groupby": ["season", "team_id", "team_name"],
                    "metrics": ["COUNT_IF(win_loss = 'W')"],
                    "orderby": [["COUNT_IF(win_loss = 'W')", False]],
                    "row_limit": 5000,
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
                print(f"[{i}/{len(chart_definitions)}] Team Chart created: '{chart_def['chart_name']}' (ID={chart_id})")
            else:
                print(
                    f"[{i}/{len(chart_definitions)}] Failed to create team chart '{chart_def['chart_name']}': "
                    f"{response.status_code}, {response.text}"
                )

    def run_setup(self) -> None:
        self.login()
        db_id = self.get_database_id_by_name("Trino")

        # Prepare dataset definitions
        scd_info = {
            "schema": "iceberg_nba_player_boxscores",
            "table_name": "scd_player_boxscores",
            "metrics": self.get_player_metrics_definitions()
        }
        team_info = {
            "schema": "iceberg_nba_player_boxscores",
            "table_name": "scd_team_boxscores",
            "metrics": self.get_team_metrics_definitions()
        }

        # Create each dataset
        scd_dataset_id = self.get_or_create_dataset(
            db_id=db_id,
            schema=scd_info["schema"],
            table=scd_info["table_name"],
            metrics=scd_info["metrics"]
        )

        team_dataset_id = self.get_or_create_dataset(
            db_id=db_id,
            schema=team_info["schema"],
            table=team_info["table_name"],
            metrics=team_info["metrics"]
        )

        # Create separate sets of charts
        self.create_player_charts(scd_dataset_id)
        self.create_team_charts(team_dataset_id)

        print("\nDatasets created or retrieved:")
        print(f"  scd_player_boxscores => dataset_id={scd_dataset_id}")
        print(f"  scd_team_boxscores   => dataset_id={team_dataset_id}")


def main() -> None:
    """
    Main entry point to run the NBASupersetSetup.
    Adjust SUPERSET_URL, USERNAME, PASSWORD as needed.
    """
    SUPERSET_URL = "http://superset:8098"
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
