{{ config(
    materialized = 'table'
) }}

--------------------------------------------------------------------------------
-- 1) Bring in the SCD team boxscores.
--------------------------------------------------------------------------------
WITH team AS (
    SELECT *
    FROM {{ ref('team_boxscores') }}
),

--------------------------------------------------------------------------------
-- 2) Self-join to fetch opponent stats so we can compute team shares.
--    We match on (game_id, season, season_type), but exclude the same team_id.
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

        t.home_team,
        t.away_team,

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
-- 3) Compute advanced stats & “team share” columns, rounding to 4 decimals.
--------------------------------------------------------------------------------
calc AS (
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

        home_team,
        away_team,

        opp_team_id,
        opp_team_points,
        opp_team_fga,
        opp_team_fgm,
        opp_team_fg3a,
        opp_team_fg3m,
        opp_team_fta,
        opp_team_ftm,
        opp_team_tov,
        opp_team_oreb,
        opp_team_dreb,
        opp_team_reb,
        opp_team_ast,
        opp_team_stl,
        opp_team_blk,

        ----------------------------------------------------------------
        -- 3A) Classic team advanced stats (like before).
        ----------------------------------------------------------------
        -- Team TS% = team_points / (2*(team_fga + 0.44*team_fta))
        CASE 
            WHEN (team_fga + 0.44*team_fta) = 0 THEN 0
            ELSE ROUND(team_points / (2.0 * (team_fga + 0.44 * team_fta)), 4)
        END AS team_ts_percentage,

        -- Team eFG% = (FGM + 0.5*FG3M) / FGA
        CASE
            WHEN team_fga = 0 THEN 0
            ELSE ROUND((team_fgm + 0.5 * team_fg3m) / team_fga, 4)
        END AS team_efg_percentage,

        -- Team 3PT rate = team_fg3a / team_fga
        CASE
            WHEN team_fga = 0 THEN 0
            ELSE ROUND(team_fg3a * 1.0 / team_fga, 4)
        END AS team_3pt_rate,

        -- Team TOV% = TOV / (FGA + 0.44*FTA + TOV)
        CASE
            WHEN (team_fga + 0.44*team_fta + team_tov) = 0 THEN 0
            ELSE ROUND(team_tov * 1.0 / (team_fga + 0.44*team_fta + team_tov), 4)
        END AS team_tov_percentage,

        -- Team OFF Rating = 100 * team_points / partial_possessions
        CASE
            WHEN partial_possessions = 0 THEN 0
            ELSE ROUND(100.0 * team_points / partial_possessions, 4)
        END AS team_off_rating,

        -- Team FT rate = team_fta / team_fga
        CASE
            WHEN team_fga = 0 THEN 0
            ELSE ROUND(team_fta * 1.0 / team_fga, 4)
        END AS team_ft_rate,

        ----------------------------------------------------------------
        -- 3B) "Team share" columns, i.e. team's share of the stat 
        --     relative to the total in that game. For example, 
        --     team_pct_fga = team_fga / (team_fga + opp_team_fga).
        ----------------------------------------------------------------
        CASE
          WHEN (team_fga + opp_team_fga) = 0 THEN 0
          ELSE ROUND(team_fga * 1.0 / (team_fga + opp_team_fga), 4)
        END AS team_pct_fga,

        CASE
          WHEN (team_fgm + opp_team_fgm) = 0 THEN 0
          ELSE ROUND(team_fgm * 1.0 / (team_fgm + opp_team_fgm), 4)
        END AS team_pct_fgm,

        CASE
          WHEN (team_fta + opp_team_fta) = 0 THEN 0
          ELSE ROUND(team_fta * 1.0 / (team_fta + opp_team_fta), 4)
        END AS team_pct_fta,

        CASE
          WHEN (team_ftm + opp_team_ftm) = 0 THEN 0
          ELSE ROUND(team_ftm * 1.0 / (team_ftm + opp_team_ftm), 4)
        END AS team_pct_ftm,

        CASE
          WHEN (team_oreb + opp_team_oreb) = 0 THEN 0
          ELSE ROUND(team_oreb * 1.0 / (team_oreb + opp_team_oreb), 4)
        END AS team_pct_oreb,

        CASE
          WHEN (team_dreb + opp_team_dreb) = 0 THEN 0
          ELSE ROUND(team_dreb * 1.0 / (team_dreb + opp_team_dreb), 4)
        END AS team_pct_dreb,

        CASE
          WHEN (team_reb + opp_team_reb) = 0 THEN 0
          ELSE ROUND(team_reb * 1.0 / (team_reb + opp_team_reb), 4)
        END AS team_pct_reb,

        CASE
          WHEN (team_blk + opp_team_blk) = 0 THEN 0
          ELSE ROUND(team_blk * 1.0 / (team_blk + opp_team_blk), 4)
        END AS team_pct_blk,

        CASE
          WHEN (team_fg3a + opp_team_fg3a) = 0 THEN 0
          ELSE ROUND(team_fg3a * 1.0 / (team_fg3a + opp_team_fg3a), 4)
        END AS team_pct_3pa,

        CASE
          WHEN (team_fg3m + opp_team_fg3m) = 0 THEN 0
          ELSE ROUND(team_fg3m * 1.0 / (team_fg3m + opp_team_fg3m), 4)
        END AS team_pct_3pm,

        CASE
          WHEN (team_ast + opp_team_ast) = 0 THEN 0
          ELSE ROUND(team_ast * 1.0 / (team_ast + opp_team_ast), 4)
        END AS team_pct_ast,

        CASE
          WHEN (team_stl + opp_team_stl) = 0 THEN 0
          ELSE ROUND(team_stl * 1.0 / (team_stl + opp_team_stl), 4)
        END AS team_pct_stl,

        CASE
          WHEN (team_tov + opp_team_tov) = 0 THEN 0
          ELSE ROUND(team_tov * 1.0 / (team_tov + opp_team_tov), 4)
        END AS team_pct_tov,

        CASE
          WHEN (team_points + opp_team_points) = 0 THEN 0
          ELSE ROUND(team_points * 1.0 / (team_points + opp_team_points), 4)
        END AS team_pct_pts
    FROM joined
),

--------------------------------------------------------------------------------
-- 4) Final selection: all base columns + computed advanced stats.
--------------------------------------------------------------------------------
final AS (
    SELECT
        team_id,
        team_name,
        team_abbreviation,
        season,
        season_type,
        game_id,
        game_date,
        matchup,
        home_team,
        away_team,
        win_loss,

        -- Classic advanced stats:
        team_ts_percentage,
        team_efg_percentage,
        team_3pt_rate,
        team_tov_percentage,
        team_off_rating,
        team_ft_rate,

        -- Team share stats:
        team_pct_fga,
        team_pct_fgm,
        team_pct_fta,
        team_pct_ftm,
        team_pct_oreb,
        team_pct_dreb,
        team_pct_reb,
        team_pct_blk,
        team_pct_3pa,
        team_pct_3pm,
        team_pct_ast,
        team_pct_stl,
        team_pct_tov,
        team_pct_pts
    FROM calc
)

SELECT *
FROM final