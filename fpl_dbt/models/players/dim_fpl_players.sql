select
    e.id
    ,e.first_name
    ,e.second_name
    ,e.web_name as name
    ,t.name as team_name
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
    ,e.transfers_in
    ,e.starts
    ,e.influence
    ,e.creativity
    ,e.threat
    ,e.ict_index
    ,rank() over (order by e.total_points desc) as total_points_rank
    ,rank() over (order by e.form desc) as form_percentile
    ,rank() over (order by e.goals_scored desc) as goals_scored_percentile
    ,rank() over (order by e.assists desc) as assists_percentile
    ,rank() over (order by e.clean_sheets desc) as clean_sheets_percentile
from
    {{ source('raw_fpl', 'elements') }} as e
    left join {{ source('raw_fpl', 'teams') }} as t
        on e.team = t.id
    left join {{ source('raw_fpl', 'element_types') }} as p
        on e.element_type = p.id
