-- SCD Type 2 logic for TEAM DIMENSION

{{ config(
    materialized='incremental',
    unique_key='team_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}


WITH base AS (
    /*
      1) Pull only the TEAM-LEVEL attributes + game_date (used for detecting changes over time).
         Drop all fact-level columns (season, matchup, points, etc.).
    */
    SELECT
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        t.game_date
    FROM {{ ref('team_boxscores_view') }} t
),


hash_calc AS (
    /*
      2) Compute a row_hash for change detection (only dimension attributes).
         We do NOT include game_date or other fact fields in the hash, 
         since the dimension changes only when team attributes change.
    */
    SELECT
        base.team_id,
        base.team_name,
        base.team_abbreviation,
        base.game_date,
        MD5(
          to_utf8(
            CONCAT(
                COALESCE(CAST(base.team_id AS VARCHAR), '_null_'), '-',
                COALESCE(base.team_name, '_null_'), '-',
                COALESCE(base.team_abbreviation, '_null_')
            )
          )
        ) AS row_hash
    FROM base
),

flag_new_block AS (
    /*
      3) Detect new SCD block whenever (team_name, team_abbreviation) 
         differs from the prior row for the same team_id.
    */
    SELECT
        h.*,
        LAG(h.team_name) OVER (PARTITION BY h.team_id ORDER BY h.game_date)     AS prev_team_name,
        LAG(h.team_abbreviation) OVER (PARTITION BY h.team_id ORDER BY h.game_date) AS prev_team_abbrev
    FROM hash_calc h
),

identify_blocks AS (
    /*
      4) Mark a row as 'new_block_flag=1' if the dimension attributes
         differ from the previous row (i.e., an actual change in team name or abbreviation).
    */
    SELECT
        f.*,
        CASE
            WHEN f.prev_team_name      = f.team_name
             AND f.prev_team_abbrev    = f.team_abbreviation
            THEN 0
            ELSE 1
        END AS new_block_flag
    FROM flag_new_block f
),

blocks AS (
    /*
      5) Within each team_id, do a cumulative sum of new_block_flag => block_id
         to group contiguous spans of the same dimension attribute values.
    */
    SELECT
        i.*,
        SUM(new_block_flag) OVER (
            PARTITION BY i.team_id
            ORDER BY i.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS block_id
    FROM identify_blocks i
),

block_date_ranges AS (
    /*
      6) For each block, find the earliest (MIN) and latest (MAX) game_date in that block.
      -- We'll treat those as the dimension's validity window: debut_date, final_date.
    */
    SELECT
        b.*,
        MIN(game_date) OVER (PARTITION BY team_id, block_id) AS block_debut_date,
        MAX(game_date) OVER (PARTITION BY team_id, block_id) AS block_final_date
    FROM blocks b
),

with_next_block AS (
    /*
      7) Look ahead to see if there's a "next block" start date, for closing out the current block's validity window.
    */
    SELECT
        block_date_ranges.*,
        LEAD(block_debut_date) OVER (
            PARTITION BY team_id
            ORDER BY block_debut_date
        ) AS next_block_start
    FROM block_date_ranges
),

max_game_date AS (
    SELECT MAX(game_date) AS max_gd
    FROM base
),

team_activity AS (
    /*
      8) Identify whether a team is still active (based on having the max game_date).
         If it has the overall max, we mark that row as is_current=TRUE
         unless there's already a "next block" start date for it.
    */
    SELECT
        b.team_id,
        CASE
            WHEN MAX(b.game_date) = (SELECT max_gd FROM max_game_date) THEN 1
            ELSE 0
        END AS is_still_active
    FROM base b
    GROUP BY b.team_id
),

final_data AS (
    /*
      9) Assemble final dimension data. 
         - The SCD validity window is [block_debut_date, block_final_date].
         - We do not include any fact-level fields (game stats, etc.).
         - We generate a unique surrogate key: team_key.
         - We set is_current based on presence/absence of next_block_start or team still active.
    */
    SELECT
        w.team_id,
        w.team_name,
        w.team_abbreviation,

        w.block_debut_date AS debut_date,
        w.block_final_date AS final_date,

        CASE
            WHEN w.next_block_start IS NOT NULL THEN FALSE
            WHEN a.is_still_active = 1 THEN TRUE
            ELSE FALSE
        END AS is_current,

        w.row_hash,

        -- Surrogate key example: "teamId_blockId_blockDebutDate"
        CONCAT(
            CAST(w.team_id AS VARCHAR), '_',
            CAST(w.block_id AS VARCHAR), '_',
            CAST(w.block_debut_date AS VARCHAR)
        ) AS team_key,

        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by
    FROM with_next_block w
    LEFT JOIN team_activity a
           ON w.team_id = a.team_id
)

    SELECT
        team_id,
        team_name,
        team_abbreviation,
        debut_date,
        final_date,
        is_current,
        row_hash,
        team_key,
        record_created_timestamp,
        record_updated_timestamp,
        record_updated_by
    FROM final_data

