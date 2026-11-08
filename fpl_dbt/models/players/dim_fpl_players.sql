select
    e.id as player_id
    ,e.first_name
    ,e.second_name
    ,e.web_name as name
    ,t.name as team_name
    ,'/'||replace(lower(t.name), ' ', '_')||'.svg' as image_path
    ,t.short_name as team_short_name
    ,p.singular_name as position
    ,p.singular_name_short as position_short
    ,e.news
    ,e.total_points
    ,e.selected_by_percent
    ,e.in_dreamteam
    ,round(e.now_cost / 10.0, 1) as cost
    ,e.form
    ,e.points_per_game
    ,e.minutes
    ,e.goals_scored
    ,e.assists
    ,e.clean_sheets
    ,e.goals_conceded
    ,e.yellow_cards
    ,e.red_cards
    ,e.saves
    ,e.bonus
    ,e.defensive_contribution
    ,e.transfers_in
    ,e.starts
    ,e.influence
    ,e.creativity
    ,e.threat
    ,e.ict_index
    ,e.expected_goals
    ,e.expected_assists
    ,e.ep_next as expected_points_next_gw
    ,rank() over (partition by e.element_type order by e.total_points desc) as total_points_rank
    ,rank() over (partition by e.element_type order by e.form desc) as form_rank
    ,rank() over (partition by e.element_type order by e.goals_scored desc) as goals_scored_rank
    ,rank() over (partition by e.element_type order by e.assists desc) as assists_rank
    ,rank() over (partition by e.element_type order by e.clean_sheets desc) as clean_sheets_rank
    ,case when e.minutes > 0 then rank() over (partition by e.element_type, (e.minutes > 0) order by e.goals_conceded asc)
            else null
        end as goals_conceded_rank
    ,rank() over (partition by e.element_type order by e.expected_goals desc) as expected_goals_rank
    ,rank() over (partition by e.element_type order by e.expected_assists desc) as expected_assists_rank
    ,rank() over (partition by e.element_type order by e.defensive_contribution desc) as defensive_contribution_rank
    ,rank() over (partition by e.element_type order by e.influence desc) as influence_rank
    ,rank() over (partition by e.element_type order by e.creativity desc) as creativity_rank
    ,rank() over (partition by e.element_type order by e.threat desc) as threat_rank
    ,rank() over (partition by e.element_type order by e.ict_index desc) as ict_index_rank
    ,rank() over (partition by e.element_type order by e.points_per_game desc) as points_per_game_rank
    ,rank() over (partition by e.element_type order by e.bonus desc) as bonus_rank
    ,rank() over (partition by e.element_type order by e.saves desc) as saves_rank
    ,now() at time zone 'Europe/Berlin' as last_updated_ts
from
    {{ source('raw_fpl', 'elements') }} as e
    left join {{ source('raw_fpl', 'teams') }} as t
        on e.team = t.id
    left join {{ source('raw_fpl', 'element_types') }} as p
        on e.element_type = p.id
