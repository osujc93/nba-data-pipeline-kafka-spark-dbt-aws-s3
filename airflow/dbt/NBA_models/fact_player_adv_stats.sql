{{ config(
    materialized='incremental',
    unique_key='fact_player_adv_key',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}


--------------------------------------------------------------------------------
-- 1) Bring in the player SCD data + team boxscores for advanced calculations.
--------------------------------------------------------------------------------
WITH player AS (
    SELECT * 
    FROM {{ ref('player_boxscores_view') }}
),

team AS (
    SELECT * 
    FROM {{ ref('fact_team_boxscores') }}
),

--------------------------------------------------------------------------------
-- 2) Join them to compute advanced stats.
--------------------------------------------------------------------------------
joined AS (
    SELECT
        p.player_id,
        p.player_name,
        p.team_id,
        p.team_name,
        p.game_id,
        p.season,
        p.season_type,
        p.game_date,
        p.matchup,
        p.win_loss,
        p.minutes_played,
        p.field_goals_attempted             AS fga,
        p.field_goals_made                  AS fgm,
        p.three_point_field_goals_attempted AS fg3a,
        p.three_point_field_goals_made      AS fg3m,
        p.free_throws_attempted             AS fta,
        p.free_throws_made                  AS ftm,
        p.turnovers                         AS tov,
        p.offensive_rebounds                AS oreb,
        p.defensive_rebounds                AS dreb,
        p.rebounds                          AS reb,
        p.assists                           AS ast,
        p.steals                            AS stl,
        p.blocks                            AS blk,
        p.points                            AS pts,
        p.plus_minus,

        -- From the team fact:
        t.team_fga,
        t.team_fgm,
        t.team_fg3a,
        t.team_fg3m,
        t.team_fta,
        t.team_ftm,
        t.team_tov,
        t.team_points,
        t.team_oreb,
        t.team_dreb,
        t.team_reb,
        t.team_ast,
        t.team_stl,
        t.team_blk,
        t.partial_possessions,

        -- Use whichever columns make sense for "home_team" / "away_team".
        -- Here we'll choose the team_boxscores columns that store these.
        t.home_team,
        t.away_team
    FROM player p
    LEFT JOIN team t
           ON p.team_id     = t.team_id
          AND p.season      = t.season
          AND p.game_id     = t.game_id
          AND p.season_type = t.season_type
),

--------------------------------------------------------------------------------
-- 3) Compute advanced stats (with rounding to 4 decimal places).
--------------------------------------------------------------------------------
calc AS (
    SELECT
        j.*,

        ----------------------------------------------------------------
        -- True Shooting %
        ----------------------------------------------------------------
        CASE 
            WHEN (fga + 0.44*fta) = 0 THEN 0
            ELSE ROUND( (pts * 1.0) / (2 * (fga + 0.44 * fta)), 4)
        END AS ts_percentage,

        ----------------------------------------------------------------
        -- eFG%
        ----------------------------------------------------------------
        CASE
            WHEN fga = 0 THEN 0
            ELSE ROUND( (fgm + 0.5 * fg3m)*1.0 / fga, 4)
        END AS efg_percentage,

        ----------------------------------------------------------------
        -- Usage %
        ----------------------------------------------------------------
        CASE
          WHEN (team_fga + 0.44 * team_fta + team_tov) = 0 THEN 0
          ELSE ROUND(
              100.0 * (fga + 0.44*fta + tov)
              / (team_fga + 0.44*team_fta + team_tov)
            , 4
          )
        END AS usg_percentage,

        ----------------------------------------------------------------
        -- %3PA
        ----------------------------------------------------------------
        CASE 
          WHEN team_fg3a = 0 THEN 0
          ELSE ROUND(fg3a*1.0 / team_fg3a, 4)
        END AS pct_3pa,

        ----------------------------------------------------------------
        -- %3PM
        ----------------------------------------------------------------
        CASE 
          WHEN team_fg3m = 0 THEN 0
          ELSE ROUND(fg3m*1.0 / team_fg3m, 4)
        END AS pct_3pm,

        ----------------------------------------------------------------
        -- %AST
        ----------------------------------------------------------------
        CASE
          WHEN team_ast = 0 THEN 0
          ELSE ROUND(ast*1.0 / team_ast, 4)
        END AS pct_ast,

        ----------------------------------------------------------------
        -- %STL
        ----------------------------------------------------------------
        CASE
          WHEN team_stl = 0 THEN 0
          ELSE ROUND(stl*1.0 / team_stl, 4)
        END AS pct_stl,

        ----------------------------------------------------------------
        -- %TOV
        ----------------------------------------------------------------
        CASE
          WHEN team_tov = 0 THEN 0
          ELSE ROUND(tov*1.0 / team_tov, 4)
        END AS pct_tov,

        ----------------------------------------------------------------
        -- %PTS
        ----------------------------------------------------------------
        CASE
          WHEN team_points = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_points, 4)
        END AS pct_pts,

        ----------------------------------------------------------------
        -- %FGA
        ----------------------------------------------------------------
        CASE
          WHEN team_fga = 0 THEN 0
          ELSE ROUND(fga*1.0 / team_fga, 4)
        END AS pct_fga,

        ----------------------------------------------------------------
        -- %FGM
        ----------------------------------------------------------------
        CASE
          WHEN team_fgm = 0 THEN 0
          ELSE ROUND(fgm*1.0 / team_fgm, 4)
        END AS pct_fgm,

        ----------------------------------------------------------------
        -- %FTA
        ----------------------------------------------------------------
        CASE
          WHEN team_fta = 0 THEN 0
          ELSE ROUND(fta*1.0 / team_fta, 4)
        END AS pct_fta,

        ----------------------------------------------------------------
        -- %FTM
        ----------------------------------------------------------------
        CASE
          WHEN team_ftm = 0 THEN 0
          ELSE ROUND(ftm*1.0 / team_ftm, 4)
        END AS pct_ftm,

        ----------------------------------------------------------------
        -- %OREB
        ----------------------------------------------------------------
        CASE
          WHEN team_oreb = 0 THEN 0
          ELSE ROUND(oreb*1.0 / team_oreb, 4)
        END AS pct_oreb,

        ----------------------------------------------------------------
        -- %DREB
        ----------------------------------------------------------------
        CASE
          WHEN team_dreb = 0 THEN 0
          ELSE ROUND(dreb*1.0 / team_dreb, 4)
        END AS pct_dreb,

        ----------------------------------------------------------------
        -- %REB
        ----------------------------------------------------------------
        CASE
          WHEN team_reb = 0 THEN 0
          ELSE ROUND(reb*1.0 / team_reb, 4)
        END AS pct_reb,

        ----------------------------------------------------------------
        -- %BLK
        ----------------------------------------------------------------
        CASE
          WHEN team_blk = 0 THEN 0
          ELSE ROUND(blk*1.0 / team_blk, 4)
        END AS pct_blk
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
              COALESCE(CAST(c.player_id AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.team_id AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.game_id AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.season AS VARCHAR), '_null_'), '-',
              COALESCE(c.season_type, '_null_'), '-',
              COALESCE(CAST(c.game_date AS VARCHAR), '_null_'), '-',
              COALESCE(c.matchup, '_null_'), '-',
              COALESCE(c.win_loss, '_null_'), '-',
              COALESCE(CAST(c.ts_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.efg_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.usg_percentage AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_fga AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_fgm AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_fta AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_ftm AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_oreb AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_dreb AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_reb AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_blk AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_3pa AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_3pm AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_ast AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_stl AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_tov AS VARCHAR), '_null_'), '-',
              COALESCE(CAST(c.pct_pts AS VARCHAR), '_null_')
            )
          )
        ) AS row_hash
    FROM calc c
),

--------------------------------------------------------------------------------
-- 5) Join dimension surrogate keys (dim_player, dim_team, dim_season, dim_game).
--------------------------------------------------------------------------------
join_dims AS (
    SELECT
        h.*,
        dp.player_key,
        dt.team_key,
        ds.season_key,
        dg.game_key
    FROM hash_calc h

    LEFT JOIN {{ ref('dim_player') }} dp
           ON  h.player_id = dp.player_id
           AND h.game_date BETWEEN dp.debut_date AND dp.final_date

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
-- 6) Final select with 'fact_player_adv_key' as the PK.
--------------------------------------------------------------------------------
final AS (
    SELECT
        j.player_id,
        j.player_name,
        j.team_id,
        j.team_name,
        j.game_id,
        j.season,
        j.season_type,
        j.game_date,
        j.matchup,
        j.home_team,
        j.away_team,
        j.win_loss,
        j.plus_minus,
        j.ts_percentage,
        j.efg_percentage,
        j.usg_percentage,
        j.pct_fga,
        j.pct_fgm,
        j.pct_fta,
        j.pct_ftm,
        j.pct_oreb,
        j.pct_dreb,
        j.pct_reb,
        j.pct_blk,
        j.pct_3pa,
        j.pct_3pm,
        j.pct_ast,
        j.pct_stl,
        j.pct_tov,
        j.pct_pts,

        j.player_key,
        j.team_key,
        j.season_key,
        j.game_key,

        j.row_hash,

        -- Primary key for this fact table:
        CONCAT(
            CAST(j.player_id AS VARCHAR), '_',
            CAST(j.season AS VARCHAR), '_',
            CAST(j.game_id AS VARCHAR)
        ) AS fact_player_adv_key,

        CURRENT_TIMESTAMP AS record_created_timestamp,
        CURRENT_TIMESTAMP AS record_updated_timestamp,
        'dbt_scd2' AS record_updated_by
    FROM join_dims j
)

SELECT * FROM final
