select
    f.id as fixture_id
    ,f.kickoff_time as kickoff_ts
    ,f.kickoff_time::date as kickoff_dt
    ,th.name as home_team_name
    ,th.short_name as home_team_name_short
    ,f.team_h_score as home_team_score
    ,f.team_h_difficulty as home_team_fdr
    ,ta.name as away_team_name
    ,ta.short_name as away_team_name_short
    ,f.team_a_score as away_team_score
    ,f.team_a_difficulty as away_team_fdr
    ,f.minutes
    ,f.finished as is_finished
from
    {{ source('raw_fpl', 'fixtures') }} as f
    left join {{ source('raw_fpl', 'teams') }} as th
        on f.team_h = th.id
    left join {{ source('raw_fpl', 'teams') }} as ta
        on f.team_a = ta.id
