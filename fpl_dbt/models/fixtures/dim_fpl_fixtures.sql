select
    f.id as fixture_id
    ,f.kickoff_time::timestamptz at time zone 'Europe/Berlin' as kickoff_ts
    ,(f.kickoff_time::timestamptz at time zone 'Europe/Berlin')::date as kickoff_dt
    ,f.event as round -- gameweek number
    ,f.team_h as home_team_id
    ,th.name as home_team_name
    ,th.short_name as home_team_name_short
    ,f.team_h_score as home_team_score
    ,f.team_h_difficulty as home_team_fdr
    ,f.team_a as away_team_id
    ,ta.name as away_team_name
    ,ta.short_name as away_team_name_short
    ,f.team_a_score as away_team_score
    ,f.team_a_difficulty as away_team_fdr
    ,f.minutes
    ,f.finished as is_finished
    ,now() at time zone 'Europe/Berlin' as last_updated_ts
from
    {{ source('raw_fpl', 'fixtures') }} as f
    left join {{ source('raw_fpl', 'teams') }} as th
        on f.team_h = th.id
    left join {{ source('raw_fpl', 'teams') }} as ta
        on f.team_a = ta.id
