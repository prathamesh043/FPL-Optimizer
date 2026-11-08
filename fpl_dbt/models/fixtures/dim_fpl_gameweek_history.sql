select
    h.element as player_id
    ,p.first_name as player_first_name
    ,p.second_name as player_second_name
    ,p.name as player_name
    ,f.kickoff_ts
    ,f.kickoff_dt
    ,h.round
    ,t.name as opposition_team_name
    ,t.short_name as opposition_team_name_short
    ,h.was_home
    ,h.team_h_score as home_team_score
    ,h.team_a_score as away_team_score
    ,case when h.was_home = true then
                    case when h.team_h_score > h.team_a_score then 'Win'
                        when h.team_h_score = h.team_a_score then 'Draw'
                        when h.team_h_score < h.team_a_score then 'Loss'
                    end
            when h.was_home = false then
                    case when h.team_h_score > h.team_a_score then 'Loss'
                        when h.team_h_score = h.team_a_score then 'Draw'
                        when h.team_h_score < h.team_a_score then 'Win'
                    end
        end as result
    ,case when h.starts = 0 then false
            when h.starts = 1 then true
        end as is_start
    ,h.minutes
    ,h.total_points
    ,h.goals_scored
    ,h.assists
    ,h.clean_sheets
    ,h.goals_conceded
    ,h.own_goals
    ,h.penalties_saved
    ,h.penalties_missed
    ,h.yellow_cards
    ,h.red_cards
    ,h.saves
    ,h.bonus
    ,h.clearances_blocks_interceptions
    ,h.tackles
    ,h.defensive_contribution
    ,h.expected_goals
    ,h.expected_assists
    ,now() at time zone 'Europe/Berlin' as last_updated_ts
from
    {{ source('raw_fpl', 'history') }} as h
    left join {{ ref('dim_fpl_players') }} as p
        on h.element = p.player_id
    left join {{ ref('dim_fpl_fixtures') }} as f
        on h.fixture = f.fixture_id
    left join {{ source('raw_fpl', 'teams') }} as t
        on h.opponent_team = t.id