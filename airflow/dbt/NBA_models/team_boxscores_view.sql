-- Aggregates team-level data from boxscores
-- Adds a GAME-LEVEL home/away mapping (computed once), then joins to TEAM-GAME aggregates.

{{ config(materialized='ephemeral') }}

WITH player AS (
    SELECT *
    FROM {{ ref('player_boxscores_view') }}
),

/* 1) Parse home/away abbreviations at GAME level (not player level). */
game_parse AS (
    SELECT
        game_id,

        -- robust-ish parsing: trim to avoid whitespace mismatch
        TRIM(SPLIT_PART(matchup, ' @ ', 1)) AS away_team_abbr_parsed,
        TRIM(SPLIT_PART(matchup, ' @ ', 2)) AS home_team_abbr_parsed

    FROM player
    GROUP BY
        game_id,
        TRIM(SPLIT_PART(matchup, ' @ ', 1)),
        TRIM(SPLIT_PART(matchup, ' @ ', 2))
),

/* 2) If a game_id produces multiple parsed pairs, pick ONE deterministically and flag it. */
game_dim AS (
    SELECT
        game_id,
        MIN(away_team_abbr_parsed) AS away_team_abbr_parsed,
        MIN(home_team_abbr_parsed) AS home_team_abbr_parsed,
        COUNT(*) AS parsed_pair_count
    FROM game_parse
    GROUP BY game_id
),

/* 3) One row per (game_id, team_abbreviation) with stable team attributes. */
teams_by_game AS (
    SELECT
        game_id,
        team_abbreviation,
        MIN(team_id)   AS team_id,
        MIN(team_name) AS team_name,
        MIN(win_loss)  AS win_loss
    FROM player
    GROUP BY game_id, team_abbreviation
),

/* 4) Build GAME-LEVEL home/away IDs by joining parsed abbr -> teams_by_game. */
game_home_away AS (
    SELECT
        g.game_id,
        g.parsed_pair_count,

        g.home_team_abbr_parsed,
        h.team_id   AS home_team_id,
        h.team_name AS home_team_name,
        h.win_loss  AS home_team_win_loss,

        g.away_team_abbr_parsed,
        a.team_id   AS away_team_id,
        a.team_name AS away_team_name,
        a.win_loss  AS away_team_win_loss

    FROM game_dim g
    LEFT JOIN teams_by_game h
        ON g.game_id = h.game_id
       AND g.home_team_abbr_parsed = h.team_abbreviation
    LEFT JOIN teams_by_game a
        ON g.game_id = a.game_id
       AND g.away_team_abbr_parsed = a.team_abbreviation
),

/*
  5) HARDENING: drop games that can't be trusted.
     - parsed_pair_count must be 1 (one unique parsed home/away per game_id)
     - home_team_id and away_team_id must both resolve
     - home_team_id != away_team_id
*/
validated_game_home_away AS (
    SELECT *
    FROM game_home_away
    WHERE parsed_pair_count = 1
      AND home_team_id IS NOT NULL
      AND away_team_id IS NOT NULL
      AND home_team_id <> away_team_id
),

/* 6) Aggregate TEAM-GAME stats first (still only grouping by team/game/season/type). */
team_aggregates AS (
    SELECT
        p.team_id,
        MIN(p.team_name)         AS team_name,
        MIN(p.team_abbreviation) AS team_abbreviation,

        p.season,
        p.game_id,
        p.season_type,

        MIN(p.game_date) AS game_date,
        MIN(p.matchup)   AS matchup,
        MIN(p.win_loss)  AS win_loss,

        -- Basic scoring stats
        SUM(p.points) AS team_points,

        -- FGA, FGM, 3PA, 3PM, FTA, FTM
        SUM(p.field_goals_attempted)             AS team_fga,
        SUM(p.field_goals_made)                  AS team_fgm,
        SUM(p.three_point_field_goals_attempted) AS team_fg3a,
        SUM(p.three_point_field_goals_made)      AS team_fg3m,
        SUM(p.free_throws_attempted)             AS team_fta,
        SUM(p.free_throws_made)                  AS team_ftm,

        -- Turnovers
        SUM(p.turnovers) AS team_tov,

        -- Rebounds
        SUM(p.offensive_rebounds) AS team_oreb,
        SUM(p.defensive_rebounds) AS team_dreb,
        SUM(p.rebounds)           AS team_reb,

        -- Assists, Steals, Blocks
        SUM(p.assists) AS team_ast,
        SUM(p.steals)  AS team_stl,
        SUM(p.blocks)  AS team_blk,

        -- partial_possessions (team side only)
        (
            SUM(p.field_goals_attempted)
            + 0.44 * SUM(p.free_throws_attempted)
            + SUM(p.turnovers)
        ) AS partial_possessions,

        -- Derived pct
        CASE WHEN SUM(p.field_goals_attempted) = 0 THEN NULL
             ELSE ROUND(SUM(p.field_goals_made) * 1.0 / SUM(p.field_goals_attempted), 3)
        END AS team_fg_pct,

        CASE WHEN SUM(p.three_point_field_goals_attempted) = 0 THEN NULL
             ELSE ROUND(SUM(p.three_point_field_goals_made) * 1.0 / SUM(p.three_point_field_goals_attempted), 3)
        END AS team_fg3_pct,

        CASE WHEN SUM(p.free_throws_attempted) = 0 THEN NULL
             ELSE ROUND(SUM(p.free_throws_made) * 1.0 / SUM(p.free_throws_attempted), 3)
        END AS team_ft_pct

    FROM player p
    GROUP BY
        p.team_id,
        p.season,
        p.game_id,
        p.season_type
),

/* 7) Join game-level home/away mapping onto team aggregates and compute flags at TEAM-GAME grain. */
final AS (
    SELECT
        t.*,

        g.home_team_id,
        g.home_team_abbr_parsed AS home_team_abbreviation,
        g.home_team_name,
        g.home_team_win_loss,

        g.away_team_id,
        g.away_team_abbr_parsed AS away_team_abbreviation,
        g.away_team_name,
        g.away_team_win_loss,

        (t.team_id = g.home_team_id) AS is_on_home_team,
        (t.team_id = g.away_team_id) AS is_on_away_team

    FROM team_aggregates t
    INNER JOIN validated_game_home_away g
        ON t.game_id = g.game_id
)

SELECT *
FROM final