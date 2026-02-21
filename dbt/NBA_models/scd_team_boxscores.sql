-- SCD Type 2 logic for team boxscores

{{ config(
    materialized='incremental',
    unique_key='team_scd_uk',
    on_schema_change='ignore',
    post_hook=[
      "
        DELETE FROM {{ this }}
        WHERE team_scd_uk IN (
            SELECT team_scd_uk
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
            team_id,
            team_name,
            team_abbreviation,
            season,
            season_type,
            game_id,
            game_date,
            matchup,
            win_loss,
            team_points,
            team_fga,
            team_fgm,
            team_fg3a,
            team_fg3m,
            team_fta,
            team_ftm,
            team_tov,
            team_oreb,
            team_dreb,
            team_reb,
            team_ast,
            team_stl,
            team_blk,
            partial_possessions,
            team_fg_pct,
            team_fg3_pct,
            team_ft_pct,
            home_team,
            away_team,
            debut_date,
            final_date,
            is_current,
            row_hash,
            team_scd_uk,
            record_created_timestamp,
            record_updated_timestamp,
            record_updated_by
        FROM {{ this }}__dbt_tmp
      "
    ]
) }}

WITH base AS (
    /*
      1) Pull raw columns from the 'team_boxscores' table.
         (Deduplicate if needed.)
    */
    SELECT
        t.team_id,
        t.team_name,
        t.team_abbreviation,

        t.season,
        t.season_type,
        t.game_id,
        t.game_date,
        t.matchup,
        t.win_loss,

        t.team_points,
        t.team_fga,
        t.team_fgm,
        t.team_fg3a,
        t.team_fg3m,
        t.team_fta,
        t.team_ftm,
        t.team_tov,
        t.team_oreb,
        t.team_dreb,
        t.team_reb,
        t.team_ast,
        t.team_stl,
        t.team_blk,
        t.partial_possessions,

        t.team_fg_pct,
        t.team_fg3_pct,
        t.team_ft_pct,

        -- home/away references
        t.home_team_abbreviation AS home_team,
        t.away_team_abbreviation AS away_team
    FROM {{ ref('team_boxscores') }} t
),

/*
   2) Compute row_hash for easier “changed row” detection in post_hook.
*/
hash_calc AS (
    SELECT
        base.*,
        MD5(
          to_utf8(
            CONCAT(
                COALESCE(CAST(base.team_id AS VARCHAR), '_null_'), '-',
                COALESCE(base.team_name, '_null_'), '-',
                COALESCE(base.team_abbreviation, '_null_'), '-',
                COALESCE(CAST(base.season AS VARCHAR), '_null_'), '-',
                COALESCE(base.season_type, '_null_'), '-',
                COALESCE(CAST(base.game_id AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.game_date AS VARCHAR), '_null_'), '-',
                COALESCE(base.matchup, '_null_'), '-',
                COALESCE(base.win_loss, '_null_'), '-',
                COALESCE(CAST(base.team_points AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fga AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fgm AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fg3a AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fg3m AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fta AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_ftm AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_tov AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_oreb AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_dreb AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_reb AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_ast AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_stl AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_blk AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.partial_possessions AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fg_pct AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_fg3_pct AS VARCHAR), '_null_'), '-',
                COALESCE(CAST(base.team_ft_pct AS VARCHAR), '_null_'), '-',
                COALESCE(base.home_team, '_null_'), '-',
                COALESCE(base.away_team, '_null_')
            )
          )
        ) AS row_hash
    FROM base
),

/*
   3) Detect "new block" for SCD whenever (team_name, team_abbreviation) changes
      vs. prior row for the same team_id.
*/
flag_new_block AS (
    SELECT
        h.*,
        LAG(h.team_name) OVER (PARTITION BY h.team_id ORDER BY h.game_date) AS prev_team_name,
        LAG(h.team_abbreviation) OVER (PARTITION BY h.team_id ORDER BY h.game_date) AS prev_team_abbrev
    FROM hash_calc h
),

identify_blocks AS (
    SELECT
        f.*,
        CASE
            WHEN f.prev_team_name = f.team_name
              AND f.prev_team_abbrev = f.team_abbreviation
            THEN 0
            ELSE 1
        END AS new_block_flag
    FROM flag_new_block f
),

/*
   4) For each team_id, cumulatively sum new_block_flag => block_id
*/
blocks AS (
    SELECT
        i.*,
        SUM(new_block_flag) OVER (
            PARTITION BY i.team_id
            ORDER BY i.game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS block_id
    FROM identify_blocks i
),

/*
   5) Earliest + latest game_date in that block => block_debut_date, block_final_date
*/
block_date_ranges AS (
    SELECT
        b.*,
        MIN(game_date) OVER (PARTITION BY team_id, block_id) AS block_debut_date,
        MAX(game_date) OVER (PARTITION BY team_id, block_id) AS block_final_date
    FROM blocks b
),

/*
   6) Identify start of next block so we know whether to set is_current=TRUE
*/
with_next_block AS (
    SELECT
        block_date_ranges.*,
        LEAD(block_debut_date) OVER (
            PARTITION BY team_id
            ORDER BY block_debut_date
        ) AS next_block_start
    FROM block_date_ranges
),

/*
   7) Check which teams are still active
*/
max_game_date AS (
    SELECT MAX(game_date) AS max_gd FROM base
),
team_activity AS (
    SELECT
        b.team_id,
        CASE
            WHEN MAX(b.game_date) = (SELECT max_gd FROM max_game_date) THEN 1
            ELSE 0
        END AS is_still_active
    FROM base b
    GROUP BY b.team_id
),

/*
   8) Final assembled data => final_data.
      Includes row_hash, derived is_current, plus a unique key team_scd_uk.
*/
final_data AS (
    SELECT
        w.team_id,
        w.team_name,
        w.team_abbreviation,
        w.season,
        w.season_type,
        w.game_id,
        w.game_date,
        w.matchup,
        w.win_loss,
        w.team_points,
        w.team_fga,
        w.team_fgm,
        w.team_fg3a,
        w.team_fg3m,
        w.team_fta,
        w.team_ftm,
        w.team_tov,
        w.team_oreb,
        w.team_dreb,
        w.team_reb,
        w.team_ast,
        w.team_stl,
        w.team_blk,
        w.partial_possessions,
        w.team_fg_pct,
        w.team_fg3_pct,
        w.team_ft_pct,
        w.home_team,
        w.away_team,

        w.block_debut_date AS debut_date,
        w.block_final_date AS final_date,

        CASE
            WHEN w.next_block_start IS NOT NULL THEN FALSE
            WHEN a.is_still_active = 1 THEN TRUE
            ELSE FALSE
        END AS is_current,

        w.row_hash,

        CONCAT(
            CAST(w.team_id AS VARCHAR), '_',
            CAST(w.block_id AS VARCHAR), '_',
            CAST(w.block_debut_date AS VARCHAR), '_',
            CAST(w.game_id AS VARCHAR)
        ) AS team_scd_uk,

        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by
    FROM with_next_block w
    LEFT JOIN team_activity a
           ON w.team_id = a.team_id
)

{% if is_incremental() %}
SELECT * FROM final_data WHERE 1=0

{% else %}
-- For a full refresh, seed all rows from final_data
SELECT
    team_id,
    team_name,
    team_abbreviation,
    season,
    season_type,
    game_id,
    game_date,
    matchup,
    win_loss,
    team_points,
    team_fga,
    team_fgm,
    team_fg3a,
    team_fg3m,
    team_fta,
    team_ftm,
    team_tov,
    team_oreb,
    team_dreb,
    team_reb,
    team_ast,
    team_stl,
    team_blk,
    partial_possessions,
    team_fg_pct,
    team_fg3_pct,
    team_ft_pct,
    home_team,
    away_team,
    debut_date,
    final_date,
    is_current,
    row_hash,
    team_scd_uk,
    record_created_timestamp,
    record_updated_timestamp,
    record_updated_by
FROM final_data

{% endif %}
