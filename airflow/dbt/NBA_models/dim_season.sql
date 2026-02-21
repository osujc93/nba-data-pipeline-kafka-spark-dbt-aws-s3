-- dim_season.sql

{{ config(
    materialized='incremental',
    unique_key='season_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}

WITH distinct_seasons AS (
    -- 1) Just get every distinct combination of (season, season_type) from the team boxscores.
    SELECT
        DISTINCT
        CAST(season AS VARCHAR) AS season_id,
        season_type
    FROM {{ ref('team_boxscores_view') }}
),

range_calc AS (
    -- 2) For each (season_id, season_type), find MIN & MAX game_date.
    SELECT
        s.season_id,
        s.season_type,
        MIN(t.game_date) AS start_date,
        MAX(t.game_date) AS end_date
    FROM distinct_seasons s
    LEFT JOIN {{ ref('team_boxscores_view') }} t
           ON  s.season_id = CAST(t.season AS VARCHAR)
           AND s.season_type = t.season_type
    GROUP BY
        s.season_id,
        s.season_type
),

final_data AS (
    -- 3) Build final dimension rows
    SELECT
        LOWER(TO_HEX(MD5(to_utf8(CONCAT(season_id, '-', season_type))))) AS season_key,
        season_id,
        CONCAT(season_id, ' - ', season_type) AS season_name,
        start_date,
        end_date,
        season_type
    FROM range_calc
)

SELECT * FROM final_data
