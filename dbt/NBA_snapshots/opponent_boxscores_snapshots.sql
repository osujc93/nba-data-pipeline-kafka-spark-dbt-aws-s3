{% snapshot opponent_boxscores_snapshot %}

{% set unique_key = 'opponent_scd_key' %}
{% set check_cols = [
    'opp_fga',
    'opp_fgm',
    'opp_fg3a',
    'opp_fg3m',
    'opp_fta',
    'opp_ftm',
    'opp_tov',
    'opp_points',
    'opp_oreb',
    'opp_dreb',
    'opp_ast',
    'opp_stl',
    'opp_blk'
] %}

{{
    config(
        target_database='nba_db',
        target_schema='nba_db',
        unique_key=unique_key,
        strategy='check',
        check_cols=check_cols
    )
}}

select
    team_id,
    game_id,
    season,
    opp_fga,
    opp_fgm,
    opp_fg3a,
    opp_fg3m,
    opp_fta,
    opp_ftm,
    opp_tov,
    opp_points,
    opp_oreb,
    opp_dreb,
    opp_ast,
    opp_stl,
    opp_blk,
    current_timestamp() as dbt_snapshot_at
from {{ ref('opponent_boxscores') }}

{% endsnapshot %}
