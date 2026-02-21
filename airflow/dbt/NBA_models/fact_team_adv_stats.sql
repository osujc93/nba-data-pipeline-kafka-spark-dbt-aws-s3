{{ config(
    materialized='incremental',
    unique_key='fact_team_adv_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}

--------------------------------------------------------------------------------
-- 1) Bring in the SCD team boxscores (or a direct view) for team-level data.
--------------------------------------------------------------------------------
WITH team AS (
    SELECT *
    FROM {{ ref('team_boxscores_view') }}
),

--------------------------------------------------------------------------------
-- 2) Self-join to fetch opponent stats so we can compute team shares.
--------------------------------------------------------------------------------
joined AS (
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

        -- Home/away columns (choose whichever your model uses):
        t.home_team_abbreviation AS home_team,
        t.away_team_abbreviation AS away_team,

        COALESCE(o.team_id,        -1) AS opp_team_id,
        COALESCE(o.team_points,    0)  AS opp_team_points,
        COALESCE(o.team_fga,       0)  AS opp_team_fga,
        COALESCE(o.team_fgm,       0)  AS opp_team_fgm,
        COALESCE(o.team_fg3a,      0)  AS opp_team_fg3a,
        COALESCE(o.team_fg3m,      0)  AS opp_team_fg3m,
        COALESCE(o.team_fta,       0)  AS opp_team_fta,
        COALESCE(o.team_ftm,       0)  AS opp_team_ftm,
        COALESCE(o.team_tov,       0)  AS opp_team_tov,
        COALESCE(o.team_oreb,      0)  AS opp_team_oreb,
        COALESCE(o.team_dreb,      0)  AS opp_team_dreb,
        COALESCE(o.team_reb,       0)  AS opp_team_reb,
        COALESCE(o.team_ast,       0)  AS opp_team_ast,
        COALESCE(o.team_stl,       0)  AS opp_team_stl,
        COALESCE(o.team_blk,       0)  AS opp_team_blk
    FROM team t
    LEFT JOIN team o
           ON  t.game_id     = o.game_id
           AND t.season      = o.season
           AND t.season_type = o.season_type
           AND t.team_id    <> o.team_id
),

--------------------------------------------------------------------------------
-- 3) Compute advanced stats & “team share” columns.
--------------------------------------------------------------------------------
calc AS (
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

        j.home_team,
        j.away_team,

        j.opp_team_id,
        j.opp_team_points,
        j.opp_team_fga,
        j.opp_team_fgm,
        j.opp_team_fg3a,
        j.opp_team_fg3m,
        j.opp_team_fta,
        j.opp_team_ftm,
        j.opp_team_tov,
        j.opp_team_oreb,
        j.opp_team_dreb,
        j.opp_team_reb,
        j.opp_team_ast,
        j.opp_team_stl,
        j.opp_team_blk,

        ----------------------------------------------------------------
        -- Classic team advanced stats.
        ----------------------------------------------------------------
        CASE 
            WHEN (j.team_fga + 0.44*j.team_fta) = 0 THEN 0
            ELSE ROUND(j.team_points / (2.0 * (j.team_fga + 0.44 * j.team_fta)), 4)
        END AS team_ts_percentage,

        CASE
            WHEN j.team_fga = 0 THEN 0
            ELSE ROUND((j.team_fgm + 0.5 * j.team_fg3m) / j.team_fga, 4)
        END AS team_efg_percentage,

        CASE
            WHEN j.team_fga = 0 THEN 0
            ELSE ROUND(j.team_fg3a * 1.0 / j.team_fga, 4)
        END AS team_3pt_rate,

        CASE
            WHEN (j.team_fga + 0.44*j.team_fta + j.team_tov) = 0 THEN 0
            ELSE ROUND(j.team_tov * 1.0 / (j.team_fga + 0.44*j.team_fta + j.team_tov), 4)
        END AS team_tov_percentage,

        CASE
            WHEN j.partial_possessions = 0 THEN 0
            ELSE ROUND(100.0 * j.team_points / j.partial_possessions, 4)
        END AS team_off_rating,

        CASE
            WHEN j.team_fga = 0 THEN 0
            ELSE ROUND(j.team_fta * 1.0 / j.team_fga, 4)
        END AS team_ft_rate,

        ----------------------------------------------------------------
        -- Team share columns (comparing vs. opp team totals)
        ----------------------------------------------------------------
        CASE
          WHEN (j.team_fga + j.opp_team_fga) = 0 THEN 0
          ELSE ROUND(j.team_fga * 1.0 / (j.team_fga + j.opp_team_fga), 4)
        END AS team_pct_fga,

        CASE
          WHEN (j.team_fgm + j.opp_team_fgm) = 0 THEN 0
          ELSE ROUND(j.team_fgm * 1.0 / (j.team_fgm + j.opp_team_fgm), 4)
        END AS team_pct_fgm,

        CASE
          WHEN (j.team_fta + j.opp_team_fta) = 0 THEN 0
          ELSE ROUND(j.team_fta * 1.0 / (j.team_fta + j.opp_team_fta), 4)
        END AS team_pct_fta,

        CASE
          WHEN (j.team_ftm + j.opp_team_ftm) = 0 THEN 0
          ELSE ROUND(j.team_ftm * 1.0 / (j.team_ftm + j.opp_team_ftm), 4)
        END AS team_pct_ftm,

        CASE
          WHEN (j.team_oreb + j.opp_team_oreb) = 0 THEN 0
          ELSE ROUND(j.team_oreb * 1.0 / (j.team_oreb + j.opp_team_oreb), 4)
        END AS team_pct_oreb,

        CASE
          WHEN (j.team_dreb + j.opp_team_dreb) = 0 THEN 0
          ELSE ROUND(j.team_dreb * 1.0 / (j.team_dreb + j.opp_team_dreb), 4)
        END AS team_pct_dreb,

        CASE
          WHEN (j.team_reb + j.opp_team_reb) = 0 THEN 0
          ELSE ROUND(j.team_reb * 1.0 / (j.team_reb + j.opp_team_reb), 4)
        END AS team_pct_reb,

        CASE
          WHEN (j.team_blk + j.opp_team_blk) = 0 THEN 0
          ELSE ROUND(j.team_blk * 1.0 / (j.team_blk + j.opp_team_blk), 4)
        END AS team_pct_blk,

        CASE
          WHEN (j.team_fg3a + j.opp_team_fg3a) = 0 THEN 0
          ELSE ROUND(j.team_fg3a * 1.0 / (j.team_fg3a + j.opp_team_fg3a), 4)
        END AS team_pct_3pa,

        CASE
          WHEN (j.team_fg3m + j.opp_team_fg3m) = 0 THEN 0
          ELSE ROUND(j.team_fg3m * 1.0 / (j.team_fg3m + j.opp_team_fg3m), 4)
        END AS team_pct_3pm,

        CASE
          WHEN (j.team_ast + j.opp_team_ast) = 0 THEN 0
          ELSE ROUND(j.team_ast * 1.0 / (j.team_ast + j.opp_team_ast), 4)
        END AS team_pct_ast,

        CASE
          WHEN (j.team_stl + j.opp_team_stl) = 0 THEN 0
          ELSE ROUND(j.team_stl * 1.0 / (j.team_stl + j.opp_team_stl), 4)
        END AS team_pct_stl,

        CASE
          WHEN (j.team_tov + j.opp_team_tov) = 0 THEN 0
          ELSE ROUND(j.team_tov * 1.0 / (j.team_tov + j.opp_team_tov), 4)
        END AS team_pct_tov,

        CASE
          WHEN (j.team_points + j.opp_team_points) = 0 THEN 0
          ELSE ROUND(j.team_points * 1.0 / (j.team_points + j.opp_team_points), 4)
        END AS team_pct_pts
    FROM joined j
),

--------------------------------------------------------------------------------
-- 4) Add row_hash for detecting changed rows in incremental runs.
--------------------------------------------------------------------------------
hash_calc AS (
    SELECT
        c.*,
        MD5(
          to_utf8(
            CONCAT(
              COALESCE(CAST(c.team_id AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.season AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.game_id AS VARCHAR), '_null_'), '-',
              COALESCE(c.season_type, '_null_'), '-',
              COALESCE(CAST(c.game_date AS VARCHAR), '_null_'), '-',
              COALESCE(c.matchup, '_null_'), '-',
              COALESCE(c.win_loss, '_null_'), '-',
              COALESCE(CAST(c.team_ts_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_efg_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_3pt_rate AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_tov_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_off_rating AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_ft_rate AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_fga AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_fgm AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_fta AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_ftm AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_oreb AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_dreb AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_reb AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_blk AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_3pa AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_3pm AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_ast AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_stl AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_tov AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_pct_pts AS VARCHAR), '_null_')
            )
          )
        ) AS row_hash
    FROM calc c
),

--------------------------------------------------------------------------------
-- 5) Join dimension surrogate keys (dim_team, dim_season, dim_game).
--------------------------------------------------------------------------------
join_dims AS (
    SELECT
        h.*,

        -- For the main team:
        dt.team_key AS main_team_key,

        -- Possibly also join to a 'dim_season', 'dim_game' if needed:
        ds.season_key,
        dg.game_key

    FROM hash_calc h

    LEFT JOIN {{ ref('dim_team') }} dt
           ON  h.team_id = dt.team_id
           AND h.game_date BETWEEN dt.debut_date AND dt.final_date

    LEFT JOIN {{ ref('dim_season') }} ds
           ON  CAST(h.season AS VARCHAR) = ds.season_id
           AND h.season_type = ds.season_type
           AND h.game_date >= ds.start_date
           AND h.game_date <= ds.end_date

    LEFT JOIN {{ ref('dim_game') }} dg
           ON  h.game_id = dg.game_id
),

--------------------------------------------------------------------------------
-- 6) Final select with 'fact_team_adv_key' as the PK.
--------------------------------------------------------------------------------
final AS (
    SELECT
        j.team_id,
        j.team_name,
        j.team_abbreviation,
        j.season,
        j.season_type,
        j.game_id,
        j.game_date,
        j.matchup,
        j.home_team,
        j.away_team,
        j.win_loss,

        j.team_ts_percentage,
        j.team_efg_percentage,
        j.team_3pt_rate,
        j.team_tov_percentage,
        j.team_off_rating,
        j.team_ft_rate,

        j.team_pct_fga,
        j.team_pct_fgm,
        j.team_pct_fta,
        j.team_pct_ftm,
        j.team_pct_oreb,
        j.team_pct_dreb,
        j.team_pct_reb,
        j.team_pct_blk,
        j.team_pct_3pa,
        j.team_pct_3pm,
        j.team_pct_ast,
        j.team_pct_stl,
        j.team_pct_tov,
        j.team_pct_pts,

        j.opp_team_id,
        j.opp_team_points,
        j.opp_team_fga,
        j.opp_team_fgm,
        j.opp_team_fg3a,
        j.opp_team_fg3m,
        j.opp_team_fta,
        j.opp_team_ftm,
        j.opp_team_tov,
        j.opp_team_oreb,
        j.opp_team_dreb,
        j.opp_team_reb,
        j.opp_team_ast,
        j.opp_team_stl,
        j.opp_team_blk,

        -- Dimension surrogate keys:
        j.main_team_key,
        j.season_key,
        j.game_key,

        j.row_hash,

        -- Primary key for this fact table:
        CONCAT(
            CAST(j.team_id AS VARCHAR), '_',
            CAST(j.season AS VARCHAR), '_',
            CAST(j.game_id AS VARCHAR)
        ) AS fact_team_adv_key,

        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by
    FROM join_dims j
)

SELECT * FROM final