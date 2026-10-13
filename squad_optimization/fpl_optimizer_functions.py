import copy

import pandas as pd


REQUIRED_POSITION_COUNTS = {
    'Goalkeeper': 2,
    'Defender': 5,
    'Midfielder': 5,
    'Forward': 3,
}


def knapsack_solution(players, player_costs, player_values, max_cost, count):

    """
    function that returns the knapsack cost matrix.

    cost_matrix[i][j][k] = best total value using exactly k players
    from players[0..i] with total cost at most j.

    Assumes player_values are strictly positive for every player (see
    adjust_player_values), so 0 always means "infeasible".
    """

    num_players = len(players)

    cost_matrix = [[[0 for k in range(count+1)] for j in range(max_cost+1)] for i in range(num_players)]

    for i in range(num_players):
        for j in range(max_cost+1):
            for k in range(count+1):
                if k == 0:
                    cost_matrix[i][j][k] = 0
                    continue

                # Skip player i
                if i == 0:
                    best = 0
                else:
                    best = cost_matrix[i-1][j][k]

                # Take player i only from a feasible (k-1)-player solution
                # (for k == 1, the empty prior solution is always feasible)
                if player_costs[i] <= j:
                    if k == 1:
                        best = max(best, player_values[i])
                    elif i > 0 and cost_matrix[i-1][j - player_costs[i]][k - 1] > 0:
                        best = max(
                            best,
                            player_values[i] + cost_matrix[i-1][j - player_costs[i]][k - 1]
                        )

                cost_matrix[i][j][k] = best

    return cost_matrix


def get_used_items(players, player_costs, player_values, max_cost, count, cost_matrix):

    """
    function that returns the used players from the cost matrix.

    Reconstructs an exactly-`count` selection. When take/skip values are tied
    (e.g. zero-metric players), prefer taking so required slots are filled.
    """

    playerIndex = len(players) - 1
    currentCount = count
    marked = [0 for _ in range(len(players))]

    bestValue = None
    currentCost = 0

    for j in range(max_cost + 1):
        value = cost_matrix[playerIndex][j][count]
        if (bestValue is None) or (value > bestValue):
            currentCost = j
            bestValue = value

    # If every player has metric 0, every budget looks equally optimal at j=0.
    # Use the full budget so reconstruction can still pick `count` players.
    if bestValue == 0 and count > 0:
        currentCost = max_cost

    while playerIndex >= 0 and currentCount > 0:
        if playerIndex == 0:
            if (
                currentCount == 1
                and player_costs[0] <= currentCost
                and (
                    cost_matrix[0][currentCost][1] == player_values[0]
                    or player_values[0] == 0
                )
            ):
                marked[0] = 1
            break

        current_val = cost_matrix[playerIndex][currentCost][currentCount]
        can_take = player_costs[playerIndex] <= currentCost

        if can_take:
            take_val = (
                player_values[playerIndex]
                + cost_matrix[playerIndex - 1][currentCost - player_costs[playerIndex]][currentCount - 1]
            )
            # Prefer taking on ties so zero-value players still fill the squad
            if current_val == take_val:
                marked[playerIndex] = 1
                currentCost -= player_costs[playerIndex]
                currentCount -= 1

        playerIndex -= 1

    return marked


def adjust_player_values(player_values):
    """
    Scale the metric and add +1 per player so the knapsack always prefers
    filling the exact required count, even when some metric values are 0.
    """
    return [float(v) * 1_000_000 + 1 for v in player_values]


def optimum_by_position(eligible_players, maximum_cost, opt_metric, position, count):
    """Select exactly `count` players for a position within `maximum_cost` (in £m)."""

    max_cost = int(maximum_cost * 10)

    pos_df = eligible_players[eligible_players['position'] == position].reset_index(drop=True)
    players = pos_df.index.tolist()
    player_costs = pos_df['now_cost'].astype(int).tolist()
    player_values = adjust_player_values(pos_df[opt_metric].fillna(0).tolist())

    empty_cols = [
        'first_name', 'second_name', 'name', 'team_name', 'team_short_name',
        'position', 'selected_by_percent', 'actual_cost', 'total_points', opt_metric
    ]

    if len(players) < count:
        return pd.DataFrame(columns=empty_cols)

    # Impossible to afford `count` players within this budget
    cheapest_total = sum(sorted(player_costs)[:count])
    if cheapest_total > max_cost:
        return pd.DataFrame(columns=empty_cols)

    cost_matrix = knapsack_solution(players, player_costs, player_values, max_cost, count)
    used_players = get_used_items(players, player_costs, player_values, max_cost, count, cost_matrix)

    player_indices = [i for i, used in enumerate(used_players) if used == 1]

    if len(player_indices) != count:
        return pd.DataFrame(columns=empty_cols)

    players_df = pos_df.iloc[player_indices]
    final = players_df[empty_cols]

    return final.loc[:, ~final.columns.duplicated()].copy()


def optimum_keepers(eligible_players, maximum_cost, opt_metric):
    return optimum_by_position(eligible_players, maximum_cost, opt_metric, 'Goalkeeper', 2)


def optimum_defence(eligible_players, maximum_cost, opt_metric):
    return optimum_by_position(eligible_players, maximum_cost, opt_metric, 'Defender', 5)


def optimum_midfield(eligible_players, maximum_cost, opt_metric):
    return optimum_by_position(eligible_players, maximum_cost, opt_metric, 'Midfielder', 5)


def optimum_attack(eligible_players, maximum_cost, opt_metric):
    return optimum_by_position(eligible_players, maximum_cost, opt_metric, 'Forward', 3)


def print_all_sum_rec(target, current_sum, start, output, result):

    """
    Function to get all the sum combinations
    """

    if current_sum == target:
        output.append(copy.copy(result))

    for i in range(start, target):
        temp_sum = current_sum + i
        if temp_sum <= target:
            result.append(i)
            print_all_sum_rec(target, temp_sum, i, output, result)
            result.pop()
        else:
            return


def print_all_sum(target):

    output = []
    result = []
    print_all_sum_rec(target, 0, 4, output, result)
    return output


def cost_breakdown(number):
    """
    Function that selects only the combinations with 4 numbers
    """
    breakdown = print_all_sum(number)
    combinations = []
    for i in breakdown:
        if len(i) == 4:
            if (i[0] >= 8) and (i[1] >= 25) and (i[2] >= 30) and (i[3] >= 20):
                combinations.append(i)
    return combinations


def best_cost_breakdown(eligible_players, opt_metric):
    """
    Function that returns the best cost breakdown (keepers - defence - midfield - attack) for the chosen metric
    """
    costs_combinations = cost_breakdown(100)

    comb_df = pd.DataFrame(columns = ['costs', 'total_cost', opt_metric, 'squad_size'])

    for costs in costs_combinations:

        gk = optimum_keepers(eligible_players, costs[0], opt_metric)
        dfnc = optimum_defence(eligible_players, costs[1], opt_metric)
        mid = optimum_midfield(eligible_players, costs[2], opt_metric)
        att = optimum_attack(eligible_players, costs[3], opt_metric)

        # Only accept full FPL squads: 2 GK, 5 DEF, 5 MID, 3 FWD
        if len(gk) != 2 or len(dfnc) != 5 or len(mid) != 5 or len(att) != 3:
            continue

        final = pd.concat([gk, dfnc, mid, att])
        total_cost = final['actual_cost'].sum()
        optimized_metric = final[opt_metric].sum()
        cost_details = [costs, total_cost, optimized_metric, len(final)]

        comb_df.loc[len(comb_df)] = cost_details

    if comb_df.empty:
        raise ValueError(
            f"No valid 15-player squad (2 GK / 5 DEF / 5 MID / 3 FWD) found for metric '{opt_metric}'."
        )

    comb_df[opt_metric] = pd.to_numeric(comb_df[opt_metric])

    return comb_df.sort_values(by=[opt_metric], ascending=False).reset_index(drop=True).head(1)


def squad_optimizer(eligible_players, opt_metric):

    """
    Final function that returns the optimized squad
    """
    costs = best_cost_breakdown(eligible_players, opt_metric)['costs'].iloc[0]

    keepers = optimum_keepers(eligible_players, costs[0], opt_metric)
    defence = optimum_defence(eligible_players, costs[1], opt_metric)
    midfield = optimum_midfield(eligible_players, costs[2], opt_metric)
    attack = optimum_attack(eligible_players, costs[3], opt_metric)

    final_squad_df = pd.concat([keepers, defence, midfield, attack]).reset_index(drop=True)

    position_counts = final_squad_df['position'].value_counts().to_dict()
    for position, required in REQUIRED_POSITION_COUNTS.items():
        actual = position_counts.get(position, 0)
        if actual != required:
            raise ValueError(
                f"Expected {required} {position}(s), got {actual}. "
                f"Squad size: {len(final_squad_df)}."
            )

    if len(final_squad_df) != 15:
        raise ValueError(f"Expected 15 players, got {len(final_squad_df)}.")

    return final_squad_df
