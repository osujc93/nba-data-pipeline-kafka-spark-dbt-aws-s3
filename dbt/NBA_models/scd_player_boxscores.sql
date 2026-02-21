-- SCD Type 2 logic for player boxscores

{{ config(
    materialized='incremental',
    unique_key='player_season_game_uk',
    on_schema_change='ignore',
    post_hook=[
      "
        DELETE FROM {{ this }}
        WHERE player_season_game_uk IN (
            SELECT player_season_game_uk
            FROM {{ this }}__dbt_tmp
        )
        AND row_hash NOT IN (
            SELECT row_hash
            FROM {{ this }}__dbt_tmp
        )
      ",

      "
        INSERT INTO {{ this }}
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
            field_goal_percentage,
            three_point_field_goals_made,
            three_point_field_goals_attempted,
            three_point_field_goal_percentage,
            free_throws_made,
            free_throws_attempted,
            free_throw_percentage,
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
            day,
            home_team,
            away_team,
            row_hash,
            player_season_game_uk,
            debut_date,
            final_date,
            is_current,
            record_created_timestamp,
            record_updated_timestamp,
            record_updated_by
        FROM {{ this }}__dbt_tmp
      "
    ]
) }}


WITH raw AS (
    /*
       1) Pull all columns from the ephemeral "player_boxscores" view.
    */
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
        field_goal_percentage,
        three_point_field_goals_made,
        three_point_field_goals_attempted,
        three_point_field_goal_percentage,
        free_throws_made,
        free_throws_attempted,
        free_throw_percentage,
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
    FROM {{ ref('player_boxscores') }}
),

deduplicated AS (
    /*
       2) Use row_number() to pick exactly one row
          if (player_id, season, game_id) repeats.
    */
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    player_id,
                    season,
                    game_id
                ORDER BY
                    game_date DESC
            ) AS rn
        FROM raw r
    ) t
    WHERE rn = 1
),

with_home_away AS (
    SELECT
        d.*,
        CASE
            WHEN d.matchup LIKE '%@%' THEN TRIM(SPLIT_PART(d.matchup, '@', 2))
            WHEN d.matchup LIKE '%vs.%' THEN TRIM(SPLIT_PART(d.matchup, 'vs.', 1))
            ELSE NULL
        END AS home_team,

        CASE
            WHEN d.matchup LIKE '%@%' THEN TRIM(SPLIT_PART(d.matchup, '@', 1))
            WHEN d.matchup LIKE '%vs.%' THEN TRIM(SPLIT_PART(d.matchup, 'vs.', 2))
            ELSE NULL
        END AS away_team
    FROM deduplicated d
),

hash_calc AS (
    SELECT
        w.*,
        MD5(
          to_utf8(
            CONCAT(
              COALESCE(CAST(w.player_id AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.team_id AS VARCHAR), '_null_'), '-',
              COALESCE(w.player_name, '_null_'), '-',
              COALESCE(w.team_abbreviation, '_null_'), '-',
              COALESCE(w.team_name, '_null_'), '-',
              COALESCE(CAST(w.game_date AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.season_id AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.game_id AS VARCHAR), '_null_'), '-',
              COALESCE(w.matchup, '_null_'), '-',
              COALESCE(w.win_loss, '_null_'), '-',
              COALESCE(CAST(w.minutes_played AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.field_goals_made AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.field_goals_attempted AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.field_goal_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.three_point_field_goals_made AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.three_point_field_goals_attempted AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.three_point_field_goal_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.free_throws_made AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.free_throws_attempted AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.free_throw_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.offensive_rebounds AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.defensive_rebounds AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.rebounds AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.assists AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.steals AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.blocks AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.turnovers AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.personal_fouls AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.points AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.plus_minus AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.fantasy_points AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.video_available AS VARCHAR), '_null_'), '-',
              COALESCE(w.season_type, '_null_'), '-',
              COALESCE(CAST(w.year AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.month AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(w.day AS VARCHAR), '_null_'), '-',
              COALESCE(w.home_team, '_null_'), '-',
              COALESCE(w.away_team, '_null_')
            )
          )
        ) AS row_hash
    FROM with_home_away w
),

player_team_intervals AS (
    SELECT
        h.*,
        CASE
            WHEN LAG(team_id) OVER (PARTITION BY player_id ORDER BY game_date) = team_id
            THEN 0
            ELSE 1
        END AS new_team_flag
    FROM hash_calc h
),

player_team_blocks AS (
    SELECT
        i.*,
        SUM(new_team_flag) OVER (
            PARTITION BY player_id
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS team_block
    FROM player_team_intervals i
),

block_date_ranges AS (
    SELECT
        b.*,
        MIN(game_date) OVER (
            PARTITION BY player_id, team_block
        ) AS block_debut_date,
        MAX(game_date) OVER (
            PARTITION BY player_id, team_block
        ) AS block_final_date
    FROM player_team_blocks b
),

final_data AS (
    SELECT
        b.season_id,
        b.player_id,
        b.player_name,
        b.team_id,
        b.team_abbreviation,
        b.team_name,
        b.game_id,
        b.game_date,
        b.matchup,
        b.win_loss,
        b.minutes_played,
        b.field_goals_made,
        b.field_goals_attempted,
        CASE
            WHEN b.field_goal_percentage IS NULL THEN 0
            WHEN b.field_goal_percentage = 1.0 THEN 100.0
            WHEN b.field_goal_percentage = 0 THEN 0
            ELSE (CAST(FLOOR((b.field_goal_percentage*1000.0)+0.5) AS DOUBLE)/1000.0)
        END AS field_goal_percentage,
        b.three_point_field_goals_made,
        b.three_point_field_goals_attempted,
        CASE
            WHEN b.three_point_field_goal_percentage IS NULL THEN 0
            WHEN b.three_point_field_goal_percentage = 1.0 THEN 100.0
            WHEN b.three_point_field_goal_percentage = 0 THEN 0
            ELSE (CAST(FLOOR((b.three_point_field_goal_percentage*1000.0)+0.5) AS DOUBLE)/1000.0)
        END AS three_point_field_goal_percentage,
        b.free_throws_made,
        b.free_throws_attempted,
        CASE
            WHEN b.free_throw_percentage IS NULL THEN 0
            WHEN b.free_throw_percentage = 1.0 THEN 100.0
            WHEN b.free_throw_percentage = 0 THEN 0
            ELSE (CAST(FLOOR((b.free_throw_percentage*1000.0)+0.5) AS DOUBLE)/1000.0)
        END AS free_throw_percentage,
        b.offensive_rebounds,
        b.defensive_rebounds,
        b.rebounds,
        b.assists,
        b.steals,
        b.blocks,
        b.turnovers,
        b.personal_fouls,
        b.points,
        COALESCE(b.plus_minus, 0) AS plus_minus,
        b.fantasy_points,
        b.video_available,
        b.season,
        b.season_type,
        b.year,
        b.month,
        b.day,
        b.home_team,
        b.away_team,
        b.row_hash,
        CONCAT(
            CAST(b.player_id AS VARCHAR), '_',
            b.season, '_',
            b.game_id
        ) AS player_season_game_uk,
        b.block_debut_date AS debut_date,
        b.block_final_date AS final_date,
        CASE
            WHEN b.block_final_date = b.game_date
                 AND b.block_final_date = (
                     SELECT MAX(game_date) FROM raw WHERE player_id = b.player_id
                   )
            THEN TRUE
            ELSE FALSE
        END AS is_current,
        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by
    FROM block_date_ranges b
)

{% if is_incremental() %}
SELECT * FROM final_data WHERE 1=0
{% else %}
SELECT * FROM final_data
{% endif %}
