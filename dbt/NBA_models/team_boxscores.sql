-- Aggregates team-level data from player_boxscores for each (team_id, season, game_id, season_type).
-- includes home/away team IDs, abbreviations, names, W/L, and flags (is_on_home_team, is_on_away_team).

{{ config(
    materialized = 'table'
) }}

WITH player AS (
    SELECT *
    FROM {{ ref('player_boxscores') }}
),

with_home_away_abbr AS (
    SELECT
        s.*,
        s.home_team AS home_team_abbr_parsed,
        s.away_team AS away_team_abbr_parsed
    FROM player s
),

/*
   2) Collapse the home team rows to exactly one per (game_id, team_abbreviation).
      We do MIN or MAX on these textual columns, but they will all match.
*/
home_teams AS (
    SELECT
        game_id,
        team_abbreviation                              AS home_team_abbr_parsed, 
        MIN(team_id)           AS home_team_id,
        MIN(team_name)         AS home_team_name,
        MIN(win_loss)          AS home_team_win_loss
    FROM player
    GROUP BY 1,2
),

/*
   3) Collapse the away team rows to exactly one per (game_id, team_abbreviation).
*/
away_teams AS (
    SELECT
        game_id,
        team_abbreviation                              AS away_team_abbr_parsed,
        MIN(team_id)           AS away_team_id,
        MIN(team_name)         AS away_team_name,
        MIN(win_loss)          AS away_team_win_loss
    FROM player
    GROUP BY 1,2
),

/*
   4) Join "with_home_away_abbr" (still at the player level) to the aggregated
      home team data for that game_id.
*/
home_join AS (
    SELECT
        w.*,
        h.home_team_id,
        h.home_team_name,
        h.home_team_win_loss,
        h.home_team_abbr_parsed AS home_team_abbreviation
    FROM with_home_away_abbr w
    LEFT JOIN home_teams h
        ON  w.game_id = h.game_id
        AND w.home_team_abbr_parsed = h.home_team_abbr_parsed
),

/*
   5) Join to the aggregated away team data similarly.
*/
away_join AS (
    SELECT
        h.*,
        a.away_team_id,
        a.away_team_name,
        a.away_team_win_loss,
        a.away_team_abbr_parsed AS away_team_abbreviation
    FROM home_join h
    LEFT JOIN away_teams a
        ON  h.game_id = a.game_id
        AND h.away_team_abbr_parsed = a.away_team_abbr_parsed
),

/*
   6) For each row (team_id = "this" team), figure out if it's the home or away team
      by comparing team_id to home_team_id or away_team_id.
*/
with_flags AS (
    SELECT
        a.*,
        CASE WHEN a.team_id = a.home_team_id THEN TRUE ELSE FALSE END AS is_on_home_team,
        CASE WHEN a.team_id = a.away_team_id THEN TRUE ELSE FALSE END AS is_on_away_team
    FROM away_join a
),

/*
   7) Now aggregate. (One row per team_id, season, game_id, season_type.)
      We SUM the numeric stats for the "this" team’s players. The new home/away columns 
      are the same within this group, so we can use MIN or MAX.
*/
team_aggregates AS (
    SELECT
        w.team_id,
        MIN(w.team_name)                   AS team_name,
        MIN(w.team_abbreviation)           AS team_abbreviation,

        w.season,
        w.game_id,
        w.season_type,

        -- Basic game context
        MIN(w.game_date)                   AS game_date,
        MIN(w.matchup)                     AS matchup,
        MIN(w.win_loss)                    AS win_loss,

        -- New: joined home/away columns
        MIN(w.home_team_id)               AS home_team_id,
        MIN(w.home_team_abbreviation)     AS home_team_abbreviation,
        MIN(w.home_team_name)             AS home_team_name,
        MIN(w.home_team_win_loss)         AS home_team_win_loss,

        MIN(w.away_team_id)               AS away_team_id,
        MIN(w.away_team_abbreviation)     AS away_team_abbreviation,
        MIN(w.away_team_name)             AS away_team_name,
        MIN(w.away_team_win_loss)         AS away_team_win_loss,

        -- Convert booleans to integer 0/1 and then take MAX()
        (MAX(CASE WHEN w.is_on_home_team THEN 1 ELSE 0 END) = 1) AS is_on_home_team,
        (MAX(CASE WHEN w.is_on_away_team THEN 1 ELSE 0 END) = 1) AS is_on_away_team,

        -- Basic scoring stats
        SUM(w.points)                     AS team_points,

        -- FGA, FGM, 3PA, 3PM, FTA, FTM
        SUM(w.field_goals_attempted)             AS team_fga,
        SUM(w.field_goals_made)                  AS team_fgm,
        SUM(w.three_point_field_goals_attempted) AS team_fg3a,
        SUM(w.three_point_field_goals_made)      AS team_fg3m,
        SUM(w.free_throws_attempted)             AS team_fta,
        SUM(w.free_throws_made)                  AS team_ftm,

        -- Turnovers
        SUM(w.turnovers)                  AS team_tov,

        -- Rebounds
        SUM(w.offensive_rebounds)         AS team_oreb,
        SUM(w.defensive_rebounds)         AS team_dreb,
        SUM(w.rebounds)                   AS team_reb,

        -- Assists, Steals, Blocks
        SUM(w.assists)                    AS team_ast,
        SUM(w.steals)                     AS team_stl,
        SUM(w.blocks)                     AS team_blk,

        -- partial_possessions = sum(FGA + 0.44*FTA + TOV) for THIS team only.
        (
            SUM(w.field_goals_attempted)
            + 0.44 * SUM(w.free_throws_attempted)
            + SUM(w.turnovers)
        )                                  AS partial_possessions,

        -- Derived columns: FG%, 3PT%, FT%
        CASE 
            WHEN SUM(w.field_goals_attempted) = 0 THEN NULL
            ELSE ROUND(
                SUM(w.field_goals_made) * 1.0 / SUM(w.field_goals_attempted),
                3
            )
        END                                AS team_fg_pct,

        CASE 
            WHEN SUM(w.three_point_field_goals_attempted) = 0 THEN NULL
            ELSE ROUND(
                SUM(w.three_point_field_goals_made) * 1.0
                / SUM(w.three_point_field_goals_attempted),
                3
            )
        END                                AS team_fg3_pct,

        CASE 
            WHEN SUM(w.free_throws_attempted) = 0 THEN NULL
            ELSE ROUND(
                SUM(w.free_throws_made) * 1.0
                / SUM(w.free_throws_attempted),
                3
            )
        END                                AS team_ft_pct

    FROM with_flags w
    GROUP BY
        w.team_id,
        w.season,
        w.game_id,
        w.season_type
)

SELECT *
FROM team_aggregates
