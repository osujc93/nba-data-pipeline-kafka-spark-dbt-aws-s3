-- fact_team_boxscores.sql

{{ config(
    materialized='incremental',
    unique_key='fact_team_game_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}

WITH base AS (
    /*
      1) Pull raw columns from the ephemeral 'team_boxscores_view'.
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

        t.home_team_abbreviation AS home_team,
        t.away_team_abbreviation AS away_team
    FROM {{ ref('team_boxscores_view') }} t
),

hash_calc AS (
    /*
       2) Compute row_hash for easier “changed row” detection in post_hook.
    */
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

flag_new_block AS (
    /*
       3) Detect "new block" for SCD whenever (team_name, team_abbreviation)
          changes vs. prior row for the same team_id.
    */
    SELECT
        h.*,
        LAG(h.team_name)         OVER (PARTITION BY h.team_id ORDER BY h.game_date) AS prev_team_name,
        LAG(h.team_abbreviation) OVER (PARTITION BY h.team_id ORDER BY h.game_date) AS prev_team_abbrev
    FROM hash_calc h
),

identify_blocks AS (
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
       4) For each team_id, cumulatively sum new_block_flag => block_id
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
       5) Earliest + latest game_date in that block => block_debut_date, block_final_date
    */
    SELECT
        b.*,
        MIN(game_date) OVER (PARTITION BY team_id, block_id) AS block_debut_date,
        MAX(game_date) OVER (PARTITION BY team_id, block_id) AS block_final_date
    FROM blocks b
),

with_next_block AS (
    /*
       6) Identify start of next block so we know whether to set is_current=TRUE
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
    SELECT MAX(game_date) AS max_gd FROM base
),

team_activity AS (
    /*
       7) Check which teams are still active
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

/*
   8) Bring in dimension surrogate keys (team, season, game).
*/
join_dims AS (
    SELECT
        w.*,

        -- Main team_key (SCD join on team_id + date range):
        dt.team_key AS main_team_key,

        -- Attempt to match home_team / away_team abbreviations:
        dt_home.team_key AS home_team_key,
        dt_away.team_key AS away_team_key,

        -- Season key:
        ds.season_key,

        -- Game key:
        dg.game_key

    FROM with_next_block w

    LEFT JOIN {{ ref('dim_team') }} dt
           ON  w.team_id = dt.team_id
           AND w.game_date BETWEEN dt.debut_date AND dt.final_date

    LEFT JOIN {{ ref('dim_team') }} dt_home
           ON  w.home_team = dt_home.team_abbreviation
           AND w.game_date BETWEEN dt_home.debut_date AND dt_home.final_date

    LEFT JOIN {{ ref('dim_team') }} dt_away
           ON  w.away_team = dt_away.team_abbreviation
           AND w.game_date BETWEEN dt_away.debut_date AND dt_away.final_date

    LEFT JOIN {{ ref('dim_season') }} ds
           ON  CAST(w.season AS VARCHAR) = ds.season_id
           AND w.season_type = ds.season_type
           AND w.game_date >= ds.start_date
           AND w.game_date <= ds.end_date

    LEFT JOIN {{ ref('dim_game') }} dg
           ON  w.game_id = dg.game_id
),

final_data AS (
    /*
       9) Final assembled data => SCD2 style. 
          Mark is_current = TRUE if there's no next_block_start
          AND the team is still active.
    */
    SELECT
        j.team_id,
        j.team_name,
        j.team_abbreviation,
        j.season,
        j.season_type,
        j.game_id,
        j.game_date,
        j.matchup,
        j.win_loss,
        j.team_points,
        j.team_fga,
        j.team_fgm,
        j.team_fg3a,
        j.team_fg3m,
        j.team_fta,
        j.team_ftm,
        j.team_tov,
        j.team_oreb,
        j.team_dreb,
        j.team_reb,
        j.team_ast,
        j.team_stl,
        j.team_blk,
        j.partial_possessions,
        j.team_fg_pct,
        j.team_fg3_pct,
        j.team_ft_pct,
        j.home_team,
        j.away_team,

        j.block_debut_date AS debut_date,
        j.block_final_date AS final_date,

        CASE
            WHEN j.next_block_start IS NOT NULL THEN FALSE
            WHEN a.is_still_active = 1 THEN TRUE
            ELSE FALSE
        END AS is_current,

        j.row_hash,

        -- Use a new, simpler “fact_team_game_key” as the unique PK:
        CONCAT(
            CAST(j.team_id AS VARCHAR), '_',
            CAST(j.block_id AS VARCHAR), '_',
            CAST(j.block_debut_date AS VARCHAR), '_',
            CAST(j.game_id AS VARCHAR)
        ) AS fact_team_game_key,

        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by,

        main_team_key,
        home_team_key,
        away_team_key,
        season_key,
        game_key
    FROM join_dims j
    LEFT JOIN team_activity a
           ON j.team_id = a.team_id
)

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
    fact_team_game_key,
    record_created_timestamp,
    record_updated_timestamp,
    record_updated_by,

    main_team_key,
    home_team_key,
    away_team_key,
    season_key,
    game_key

FROM final_data
