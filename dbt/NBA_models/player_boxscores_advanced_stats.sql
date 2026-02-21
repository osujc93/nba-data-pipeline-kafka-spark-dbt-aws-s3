{{ config(
    materialized = 'table'
) }}

--------------------------------------------------------------------------------
-- 1) Bring in the player SCD data.
--------------------------------------------------------------------------------
WITH player AS (
    SELECT * 
    FROM {{ ref('player_boxscores') }}
),

--------------------------------------------------------------------------------
-- 2) Bring in the team stats to compute "player stat / team stat".
--------------------------------------------------------------------------------
team AS (
    SELECT * 
    FROM {{ ref('team_boxscores') }}
),

--------------------------------------------------------------------------------
-- 3) Join them
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
        t.home_team AS home_team,
        t.away_team AS away_team

    FROM player p
    LEFT JOIN team t
           ON p.team_id     = t.team_id
          AND p.season      = t.season
          AND p.game_id     = t.game_id
          AND p.season_type = t.season_type
),

--------------------------------------------------------------------------------
-- 4) Compute advanced stats (with rounding to 4 decimal places).
--------------------------------------------------------------------------------
calc AS (
    SELECT
        *,
        ----------------------------------------------------------------
        -- True Shooting % (rounded)
        -- TS% = Points / [2*(FGA + 0.44*FTA)]
        ----------------------------------------------------------------
        CASE 
            WHEN (fga + 0.44*fta) = 0 THEN 0
            ELSE ROUND( (pts * 1.0) / (2 * (fga + 0.44 * fta)), 4)
        END AS ts_percentage,

        ----------------------------------------------------------------
        -- eFG% (rounded)
        -- eFG% = (FGM + 0.5*FG3M) / FGA
        ----------------------------------------------------------------
        CASE
            WHEN fga = 0 THEN 0
            ELSE ROUND( (fgm + 0.5 * fg3m)*1.0 / fga, 4)
        END AS efg_percentage,

        ----------------------------------------------------------------
        -- USG% (rounded)
        -- USG% = 100 * (FGA + 0.44*FTA + TOV) /
        --                  (Team FGA + 0.44*Team FTA + Team TOV)
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
        -- %3PA (rounded)
        -- %3PA = Player 3PA / Team 3PA
        ----------------------------------------------------------------
        CASE 
          WHEN team_fg3a = 0 THEN 0
          ELSE ROUND(fg3a*1.0 / team_fg3a, 4)
        END AS pct_3pa,

        ----------------------------------------------------------------
        -- %3PM (rounded)
        -- %3PM = Player 3PM / Team 3PM
        ----------------------------------------------------------------
        CASE 
          WHEN team_fg3m = 0 THEN 0
          ELSE ROUND(fg3m*1.0 / team_fg3m, 4)
        END AS pct_3pm,

        ----------------------------------------------------------------
        -- %AST (rounded)
        -- %AST = Player AST / Team AST
        ----------------------------------------------------------------
        CASE
          WHEN team_ast = 0 THEN 0
          ELSE ROUND(ast*1.0 / team_ast, 4)
        END AS pct_ast,

        ----------------------------------------------------------------
        -- %STL (rounded)
        -- %STL = Player STL / Team STL
        ----------------------------------------------------------------
        CASE
          WHEN team_stl = 0 THEN 0
          ELSE ROUND(stl*1.0 / team_stl, 4)
        END AS pct_stl,

        ----------------------------------------------------------------
        -- %TOV (rounded)
        -- %TOV = Player TOV / Team TOV
        ----------------------------------------------------------------
        CASE
          WHEN team_tov = 0 THEN 0
          ELSE ROUND(tov*1.0 / team_tov, 4)
        END AS pct_tov,

        ----------------------------------------------------------------
        -- %PTS (rounded)
        -- %PTS = Player PTS / Team PTS
        ----------------------------------------------------------------
        CASE
          WHEN team_points = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_points, 4)
        END AS pct_pts,

        ----------------------------------------------------------------
        -- %FGA (rounded)
        -- %FGA = Player FGA / Team FGA
        ----------------------------------------------------------------
        CASE
          WHEN team_fga = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_fga, 4)
        END AS pct_fga,

        ----------------------------------------------------------------
        -- %FGM (rounded)
        -- %FGM = Player FGM / Team FGM
        ----------------------------------------------------------------
        CASE
          WHEN team_fgm = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_fgm, 4)
        END AS pct_fgm,

        ----------------------------------------------------------------
        -- %FTM (rounded)
        -- %FTM = Player FTM / Team FTM
        ----------------------------------------------------------------
        CASE
          WHEN team_ftm = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_ftm, 4)
        END AS pct_ftm,

        ----------------------------------------------------------------
        -- %FTA (rounded)
        -- %FTA = Player FTA / Team FTA
        ----------------------------------------------------------------
        CASE
          WHEN team_points = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_points, 4)
        END AS pct_fta,

        ----------------------------------------------------------------
        -- %OREB (rounded)
        -- %OREB = Player OREB / Team OREB
        ----------------------------------------------------------------
        CASE
          WHEN team_oreb = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_oreb, 4)
        END AS pct_oreb,

        ----------------------------------------------------------------
        -- %DREB (rounded)
        -- %DREB = Player DREB / Team DREB
        ----------------------------------------------------------------
        CASE
          WHEN team_dreb = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_dreb, 4)
        END AS pct_dreb,

        ----------------------------------------------------------------
        -- %REB (rounded)
        -- %REB = Player REB / Team REB
        ----------------------------------------------------------------
        CASE
          WHEN team_reb = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_reb, 4)
        END AS pct_reb,

        ----------------------------------------------------------------
        -- %BLK (rounded)
        -- %BLK = Player BLK / Team BLK
        ----------------------------------------------------------------
        CASE
          WHEN team_blk = 0 THEN 0
          ELSE ROUND(pts*1.0 / team_blk, 4)
        END AS pct_blk
    FROM joined
),

--------------------------------------------------------------------------------
-- 5) Return everything plus computed advanced columns
--------------------------------------------------------------------------------
final AS (
    SELECT
        player_id,
        player_name,
        team_id,
        team_name,
        game_id,
        season,
        season_type,
        game_date,
        matchup,
        home_team,
        away_team,
        win_loss,
        plus_minus,
        ts_percentage,
        efg_percentage,
        usg_percentage,
        pct_fga,
        pct_fgm,
        pct_fta,
        pct_ftm,
        pct_oreb,
        pct_dreb
        pct_reb,
        pct_blk,
        pct_3pa,
        pct_3pm,
        pct_ast,
        pct_stl,
        pct_tov,
        pct_pts
    FROM calc
)

SELECT *
FROM final