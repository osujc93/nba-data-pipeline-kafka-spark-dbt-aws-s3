{% snapshot team_boxscores_snapshot %}

{% set unique_key = 'team_scd_key' %}
{% set check_cols = [
    'team_points',
    'team_fga',
    'team_fta',
    'team_tov',
    'partial_possessions'
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
    season,
    game_id,
    team_points,
    team_fga,
    team_fta,
    team_tov,
    partial_possessions,
    current_timestamp() as dbt_snapshot_at
from {{ ref('team_boxscores') }}

{% endsnapshot %}
