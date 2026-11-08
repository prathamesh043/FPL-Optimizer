-- for every player, a list of all their upcoming fixtures
with upcoming_fixtures as 
(
    select
        p.player_id
        ,f.kickoff_ts
        ,true as is_home
        ,f.round
        ,f.away_team_id as opposition_team_id
        ,f.away_team_name as opposition_team_name
        ,f.away_team_name_short as opposition_team_name_short
        ,f.home_team_fdr as fixture_difficulty_rating
    from
        {{ ref('dim_fpl_players') }} as p
        left join {{ ref('dim_fpl_fixtures') }} as f
            on p.team_id = f.home_team_id
    where
        f.kickoff_ts > now() at time zone 'Europe/Berlin'

    union all

    select
        p.player_id
        ,f.kickoff_ts
        ,false as is_home
        ,f.round
        ,f.home_team_id as opposition_team_id
        ,f.home_team_name as opposition_team_name
        ,f.home_team_name_short as opposition_team_name_short
        ,f.away_team_fdr as fixture_difficulty_rating
    from
        {{ ref('dim_fpl_players') }} as p
        left join {{ ref('dim_fpl_fixtures') }} as f
            on p.team_id = f.away_team_id
    where
        f.kickoff_ts > now() at time zone 'Europe/Berlin'
)

select
    row_number() over (partition by player_id order by kickoff_ts asc) as fixture_n
    ,player_id
    ,kickoff_ts
    ,is_home
    ,round
    ,opposition_team_id
    ,opposition_team_name
    ,opposition_team_name_short
    ,fixture_difficulty_rating
from
    upcoming_fixtures