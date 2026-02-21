-- SCD Type 2 logic for Player Dimension

{{ config(
    materialized='incremental',
    unique_key='player_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}


WITH raw AS (
    /*
       1) Pull all columns from the ephemeral "boxscores_view".
          Even though we only ultimately keep dimension columns,
          we "keep the structure" and still select everything here.
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
    FROM {{ ref('player_boxscores_view') }}
),

deduplicated AS (
    /*
       2) Use row_number() to pick exactly one row if (player_id, season, game_id) repeats.
          Again, we keep the flow, even though dimension-level logic won’t rely on game-level data.
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
    /*
       3) We keep this step as-is, though it's more relevant for fact data.
          (You might remove it entirely for a pure dimension,
           but here we keep it to honor "keep all structure and flow the same".)
    */
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
    /*
       4) Compute a hash, but ONLY from the dimension attributes 
          we care about for SCD2 (e.g. player_id, player_name, team_id, team_abbreviation, team_name).
          We remove game-level columns from the concatenation.
    */
    SELECT
        w.*,
        MD5(
          to_utf8(
            CONCAT(
              COALESCE(CAST(w.player_id AS VARCHAR), '_null_'), '-',
              COALESCE(w.player_name, '_null_'), '-',
              COALESCE(CAST(w.team_id AS VARCHAR), '_null_'), '-',
              COALESCE(w.team_abbreviation, '_null_'), '-',
              COALESCE(w.team_name, '_null_')
            )
          )
        ) AS row_hash
    FROM with_home_away w
),

player_team_intervals AS (
    /*
       5) Detect changes in the player's team (if you consider team part of the dimension).
          We keep the same logic, but it’s effectively how we split SCD blocks by team changes.
    */
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
    /*
       6) Summation over the partition to identify each contiguous block of team affiliation.
    */
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
    /*
       7) For each block, identify the earliest date (block_debut_date) and latest date (block_final_date).
          We keep the "structure" even though we’ll store only dimension data in final.
    */
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
    /*
       8) Final SELECT for the dimension table. 
          We remove all fact‐level columns (game_id, game_date, stats, etc.) 
          and keep only dimension columns plus SCD2 fields.
    */
    SELECT
        -- Surrogate key for the new dimension version:
        MD5(
            to_utf8(
                CONCAT(
                    COALESCE(CAST(b.player_id AS VARCHAR), '_null_'), '-',
                    COALESCE(CAST(b.team_block AS VARCHAR), '_null_'), '-',
                    COALESCE(CAST(b.block_debut_date AS VARCHAR), '_null_')
                )
            )
        ) AS player_key,
        b.player_id,
        b.player_name,
        b.team_id,
        b.team_abbreviation,
        b.team_name,

        -- The row_hash from dimension attributes
        b.row_hash,

        -- SCD2 Effective date range
        b.block_debut_date AS debut_date,
        b.block_final_date AS final_date,

        -- is_current (true if it's the last block for this player)
        CASE
            WHEN b.block_final_date = b.game_date
                 AND b.block_final_date = (
                     SELECT MAX(game_date) FROM raw WHERE player_id = b.player_id
                   )
            THEN TRUE
            ELSE FALSE
        END AS is_current,

        -- Auditing columns
        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by
    FROM block_date_ranges b
)

SELECT * FROM final_data