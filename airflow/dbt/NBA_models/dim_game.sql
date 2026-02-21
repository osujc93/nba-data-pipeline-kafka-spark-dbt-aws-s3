-- dim_game.sql

{{ config(
    materialized='incremental',
    unique_key='game_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}

WITH a AS (
    SELECT 
        md5(
            to_utf8(
                CAST(game_id AS VARCHAR))) AS game_key, 
        game_id, 
        game_date, 
        matchup, 
        SPLIT_PART(matchup, ' @ ', 1) away_team, 
        SPLIT_PART(matchup, ' @ ', 2) home_team, 
        season_id, 
        season, 
        season_type 
        FROM {{ ref('player_boxscores_view') }} 
        WHERE matchup LIKE '%@%'
        ) 
        
        SELECT 
        * 
        FROM a 
        GROUP BY game_key, 
        game_id, 
        game_date, 
        matchup, 
        away_team, 
        home_team, 
        season_id, 
        season, 
        season_type

