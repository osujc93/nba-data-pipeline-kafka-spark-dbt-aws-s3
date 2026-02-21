-- simple reference to the base table using 'view' materialization.

{{ config(
    materialized='view'
) }}

SELECT
    season_id,
    player_id,
    player_name,
    team_id,
    team_abbreviation,
    team_name,
    game_id,
    game_date,
    matchup,
    win_loss,
    minutes_played,
    field_goals_made,
    field_goals_attempted,

    -- casts to ensure numeric/double type
    CAST(field_goal_percentage AS DOUBLE) AS field_goal_percentage,
    three_point_field_goals_made,
    three_point_field_goals_attempted,
    CAST(three_point_field_goal_percentage AS DOUBLE) AS three_point_field_goal_percentage,
    free_throws_made,
    free_throws_attempted,
    CAST(free_throw_percentage AS DOUBLE) AS free_throw_percentage,

    offensive_rebounds,
    defensive_rebounds,
    rebounds,
    assists,
    steals,
    blocks,
    turnovers,
    personal_fouls,
    points,
    plus_minus,
    fantasy_points,
    video_available,
    season,
    season_type,
    year,
    month,
    day

FROM iceberg_nba_player_boxscores.nba_player_boxscores
