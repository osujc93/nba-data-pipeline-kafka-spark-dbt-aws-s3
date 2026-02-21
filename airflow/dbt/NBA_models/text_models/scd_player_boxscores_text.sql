-- File: NBA_models/text_models/scd_player_boxscores_text.sql
-- Converts all numerical data from scd_player_boxscores into rich, detailed sentences,
-- referencing the player by their actual name. Now includes home/away team text fields
-- and textual interpretations of win_loss, year, month, day, season_type, etc.

{{ config(
    materialized='table'
) }}

WITH base AS (
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
        day,
        home_team,
        away_team,
        debut_date,
        final_date,
        fact_player_game_key
    FROM {{ ref('fact_player_boxscores') }}
)

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

    -- Textual columns describing the player's stats
    CASE
        WHEN minutes_played = 0 
            THEN CONCAT(
                player_name,
                ' of the ',
                team_name,
                ' did not play in ',
                matchup,
                ' on ',
                game_date,
                '. He stayed on the bench for the entire game.'
            )
        WHEN minutes_played = 1
            THEN CONCAT(
                player_name,
                ' of the ',
                team_name,
                ' played for only 1 minute in ',
                matchup,
                ' on ',
                game_date
            )
        WHEN minutes_played IS NOT NULL 
            THEN CONCAT(
                player_name,
                ' of the ',
                team_name,
                ' played ',
                minutes_played,
                ' minute(s) in ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL
    END AS minutes_played_text,

    CASE 
        WHEN field_goals_made IS NOT NULL 
            THEN CONCAT(
                player_name, 
                ' made ', 
                field_goals_made, 
                ' field goal(s) in ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS field_goals_made_text,

    CASE 
        WHEN field_goals_attempted IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' attempted ', 
                field_goals_attempted, 
                ' field goal(s) in ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS field_goals_attempted_text,

    CASE 
        WHEN field_goal_percentage IS NOT NULL
            THEN CONCAT(
                player_name, 
                "'s field goal percentage stood at ", 
                field_goal_percentage, 
                '%, reflecting shooting efficiency. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS field_goal_percentage_text,

    CASE 
        WHEN three_point_field_goals_made IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' nailed ', 
                three_point_field_goals_made, 
                ' three-pointer(s), stretching the defense effectively. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS three_point_field_goals_made_text,

    CASE 
        WHEN three_point_field_goals_attempted IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' took ', 
                three_point_field_goals_attempted, 
                ' shot(s) from beyond the arc, emphasizing perimeter play. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS three_point_field_goals_attempted_text,

    CASE 
        WHEN three_point_field_goal_percentage IS NOT NULL
            THEN CONCAT(
                player_name, 
                "'s three-point accuracy reached ", 
                three_point_field_goal_percentage, 
                '%, adding depth to the offense. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS three_point_field_goal_percentage_text,

    CASE 
        WHEN free_throws_made IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' converted ', 
                free_throws_made, 
                ' free throw(s), capitalizing on opportunities at the line. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS free_throws_made_text,

    CASE 
        WHEN free_throws_attempted IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' attempted ', 
                free_throws_attempted, 
                ' free throw(s), showing willingness to draw fouls. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS free_throws_attempted_text,

    CASE 
        WHEN free_throw_percentage IS NOT NULL
            THEN CONCAT(
                player_name, 
                "'s free throw percentage stood at ", 
                free_throw_percentage, 
                '%, demonstrating composure at the charity stripe. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS free_throw_percentage_text,

    CASE 
        WHEN offensive_rebounds IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' grabbed ', 
                offensive_rebounds, 
                ' offensive rebound(s), creating second-chance scoring opportunities. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS offensive_rebounds_text,

    CASE 
        WHEN defensive_rebounds IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' secured ', 
                defensive_rebounds, 
                ' defensive board(s), preventing additional opponent possessions. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS defensive_rebounds_text,

    CASE 
        WHEN rebounds IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' collected ', 
                rebounds, 
                ' total rebound(s), dominating the glass. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS rebounds_text,

    CASE 
        WHEN assists IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' distributed ', 
                assists, 
                ' assist(s), facilitating efficient ball movement. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS assists_text,

    CASE 
        WHEN steals IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' recorded ', 
                steals, 
                ' steal(s), disrupting the opposing offense. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS steals_text,

    CASE 
        WHEN blocks IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' tallied ', 
                blocks, 
                ' block(s), protecting the rim effectively. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS blocks_text,

    CASE 
        WHEN turnovers IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' committed ', 
                turnovers, 
                ' turnover(s), reflecting occasional lapses in ball security. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS turnovers_text,

    CASE 
        WHEN personal_fouls IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' was called for ', 
                personal_fouls, 
                ' personal foul(s), highlighting a physical style of play. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS personal_fouls_text,

    CASE 
        WHEN points IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' racked up ', 
                points, 
                ' point(s), serving as a key scoring option. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS points_text,

    CASE 
        WHEN plus_minus IS NOT NULL
            THEN CONCAT(
                player_name, 
                "'s plus/minus rating was ", 
                plus_minus, 
                ', indicating the team’s performance during time on the floor. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS plus_minus_text,

    CASE 
        WHEN fantasy_points IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' amassed ', 
                fantasy_points, 
                ' fantasy point(s), showcasing robust overall production. ',
                matchup,
                ' on ',
                game_date
            )
        ELSE NULL 
    END AS fantasy_points_text,

    CASE 
        WHEN debut_date IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' made his debut for ', 
                team_name, 
                ' on ',
                debut_date
            )
        ELSE NULL 
    END AS debut_date_text,

    CASE 
        WHEN final_date IS NOT NULL
            THEN CONCAT(
                player_name, 
                ' played his final game for ', 
                team_name, 
                ' on ',
                final_date
            )
        ELSE NULL 
    END AS final_date_text,

    /*
      New textual columns for home/away teams, win/loss, and date fields
    */
    CASE 
        WHEN home_team IS NOT NULL 
            THEN CONCAT('Home team is ', home_team)
        ELSE 'Home team is unknown'
    END AS home_team_text,

    CASE 
        WHEN away_team IS NOT NULL 
            THEN CONCAT('Away team is ', away_team)
        ELSE 'Away team is unknown'
    END AS away_team_text,

    CASE 
        WHEN win_loss = 'W' THEN CONCAT(team_name, ' won the game.')
        WHEN win_loss = 'L' THEN CONCAT(team_name, ' lost the game.')
        ELSE CONCAT(team_name, ' outcome is unknown (', win_loss, ').')
    END AS win_loss_text,

    -- Translate numeric month into textual name (1->January, 2->February, etc.)
    CASE month
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
        ELSE 'Unknown month'
    END AS month_text,

    -- Adds ordinal suffix to day: 1->1st, 2->2nd, 3->3rd, 11->11th, etc.
    CASE
        WHEN day % 100 IN (11, 12, 13) THEN CONCAT(day, 'th')
        WHEN day % 10 = 1 THEN CONCAT(day, 'st')
        WHEN day % 10 = 2 THEN CONCAT(day, 'nd')
        WHEN day % 10 = 3 THEN CONCAT(day, 'rd')
        ELSE CONCAT(day, 'th')
    END AS day_text,

    CONCAT('Year ', CAST(year AS STRING), ' of ', season) AS year_text,

    CASE 
        WHEN season_type = 'regular_season' THEN CONCAT('Regular season of ', season)
        WHEN season_type = 'preseason' THEN CONCAT('Preseason of ', season)
        WHEN season_type = 'playoffs' THEN CONCAT('Playoffs of ', season)
        ELSE CONCAT(season_type, ' of ', season)
    END AS season_type_text,

    video_available,
    season,
    season_type,
    year,
    month,
    day,
    fact_player_game_key

FROM base
;
