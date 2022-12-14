import requests
import pandas as pd
import numpy as np
import copy


def knapsack_solution(players, player_costs, player_values, max_cost, count):
    
    """
    function that returns the knapsack cost matrix
    """
  
    num_players = len(players)
  
    cost_matrix = [[[0 for k in range(count+1)] for j in range(max_cost+1)] for i in range(num_players)]
    
    for i in range(num_players):
        for j in range(max_cost+1):
            for k in range(count+1):
                if (player_costs[i] > j) or (1 > k):
                    cost_matrix[i][j][k] = cost_matrix[i-1][j][k]
                else: 
                    cost_matrix[i][j][k] = max(cost_matrix[i-1][j][k], player_values[i]+cost_matrix[i-1][j-player_costs[i]][k-1])

    return cost_matrix
    


def get_used_items(players, player_costs, player_values, max_cost, count, cost_matrix):
    
    """
    function that returns the used players from the cost matrix
    """
    
    playerIndex = len(players) - 1
    
    currentCost = -1
    currentCount = count
    marked = [0 for k in range(len(players))]

    bestValue = -1
    
    for j in range(max_cost+1):
        value = cost_matrix[playerIndex][j][count]
        if (bestValue == -1) or (value > bestValue):
            currentCost = j
            bestValue = value
    
    while (playerIndex >= 0 and currentCost >= 0 and currentCount >= 0):
        if (playerIndex == 0 and cost_matrix[playerIndex][currentCost][currentCount] > 0) or (cost_matrix[playerIndex][currentCost][currentCount] != cost_matrix[playerIndex-1][currentCost][currentCount]):
            marked[playerIndex] = 1
            currentCost = currentCost - player_costs[playerIndex]
            currentCount = currentCount - 1
        playerIndex = playerIndex - 1

    return marked
      

def optimum_keepers(eligible_players, maximum_cost, opt_metric):
    
    """
    function that returns the best keepers given a max cost and metric to be optimized
    """
    max_cost = maximum_cost * 10
    
    gk_df = eligible_players[eligible_players['position'] == 'Goalkeeper']
    gk_df = gk_df.reset_index()
    goalkeepers = gk_df.index.tolist()
    goalkeeper_costs = (gk_df['now_cost']).tolist()
    goalkeeper_values = gk_df[opt_metric].tolist()
    
    cost_matrix = knapsack_solution(goalkeepers, goalkeeper_costs, goalkeeper_values, max_cost, 2)
    
    used_players = get_used_items(goalkeepers, goalkeeper_costs, goalkeeper_values, max_cost, 2, cost_matrix)
    
    player_indices = []
    
    for i in range(len(used_players)):
        if used_players[i] == 1:
            player_indices.append(i)
        
    players = pd.DataFrame()
    
    for index in range(len(player_indices)):
        players = pd.concat([players, gk_df.iloc[[player_indices[index]]]])
        
    final = players[['first_name', 'second_name', 'team_name', 'position', 'selected_by_percent', 'actual_cost', 'total_points', opt_metric]]
    
    return final.loc[:,~final.columns.duplicated()].copy()


def optimum_defence(eligible_players, maximum_cost, opt_metric):

    """
    function that returns the best defenders given a max cost and metric to be optimized
    """
    max_cost = maximum_cost * 10
    
    def_df = eligible_players[eligible_players['position'] == 'Defender']
    def_df = def_df.reset_index()
    defenders = def_df.index.tolist()
    defender_costs = (def_df['now_cost']).tolist()
    defender_values = def_df[opt_metric].tolist()
    
    cost_matrix = knapsack_solution(defenders, defender_costs, defender_values, max_cost, 5)
    
    used_players = get_used_items(defenders, defender_costs, defender_values, max_cost, 5, cost_matrix)
    
    player_indices = []
    
    for i in range(len(used_players)):
        if used_players[i] == 1:
            player_indices.append(i)
        
    players = pd.DataFrame()
    
    for index in range(len(player_indices)):
        players = pd.concat([players, def_df.iloc[[player_indices[index]]]])
        
    final = players[['first_name', 'second_name', 'team_name', 'position', 'selected_by_percent', 'actual_cost', 'total_points', opt_metric]]
    
    return final.loc[:,~final.columns.duplicated()].copy()


def optimum_midfield(eligible_players, maximum_cost, opt_metric):
    
    """
    function that returns the best midfielders given a max cost and metric to be optimized
    """
    max_cost = maximum_cost * 10
    
    mid_df = eligible_players[eligible_players['position'] == 'Midfielder']
    mid_df = mid_df.reset_index()
    midfielders = mid_df.index.tolist()
    midfielder_costs = (mid_df['now_cost']).tolist()
    midfielder_values = mid_df[opt_metric].tolist()
    
    cost_matrix = knapsack_solution(midfielders, midfielder_costs, midfielder_values, max_cost, 5)
    
    used_players = get_used_items(midfielders, midfielder_costs, midfielder_values, max_cost, 5, cost_matrix)
    
    player_indices = []
    
    for i in range(len(used_players)):
        if used_players[i] == 1:
            player_indices.append(i)
        
    players = pd.DataFrame()
    
    for index in range(len(player_indices)):
        players = pd.concat([players, mid_df.iloc[[player_indices[index]]]])
        
    final = players[['first_name', 'second_name', 'team_name', 'position', 'selected_by_percent', 'actual_cost', 'total_points', opt_metric]]
    
    return final.loc[:,~final.columns.duplicated()].copy()


def optimum_attack(eligible_players, maximum_cost, opt_metric):
    
    """
    function that returns the best attackers given a max cost and metric to be optimized
    """
    max_cost = maximum_cost * 10
    
    att_df = eligible_players[eligible_players['position'] == 'Forward']
    att_df = att_df.reset_index()
    attackers = att_df.index.tolist()
    attacker_costs = (att_df['now_cost']).tolist()
    attacker_values = att_df[opt_metric].tolist()
    
    cost_matrix = knapsack_solution(attackers, attacker_costs, attacker_values, max_cost, 3)
    
    used_players = get_used_items(attackers, attacker_costs, attacker_values, max_cost, 3, cost_matrix)
    
    player_indices = []
    
    for i in range(len(used_players)):
        if used_players[i] == 1:
            player_indices.append(i)
        
    players = pd.DataFrame()
    
    for index in range(len(player_indices)):
        players = pd.concat([players, att_df.iloc[[player_indices[index]]]])
        
    final = players[['first_name', 'second_name', 'team_name', 'position', 'selected_by_percent', 'actual_cost', 'total_points', opt_metric]]
    
    return final.loc[:,~final.columns.duplicated()].copy()


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

    comb_df = pd.DataFrame(columns = ['costs', 'total_cost', opt_metric])
    
    for costs in costs_combinations:
        
        gk = optimum_keepers(eligible_players, costs[0], opt_metric)
        dfnc = optimum_defence(eligible_players, costs[1], opt_metric)
        mid = optimum_midfield(eligible_players, costs[2], opt_metric)
        att = optimum_attack(eligible_players, costs[3], opt_metric)
        
        final = pd.concat([gk, dfnc, mid, att])
        total_cost = final['actual_cost'].sum()
        optimized_metric = final[opt_metric].sum()
        cost_details = [costs, total_cost, optimized_metric]
        
        comb_df.loc[len(comb_df)] = cost_details

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

    final_squad = [keepers, defence, midfield, attack]

    final_squad_df = pd.concat(final_squad).reset_index(drop=True)

    return final_squad_df