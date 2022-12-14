import requests
import pandas as pd
import numpy as np
import copy

import psycopg2
from sqlalchemy import create_engine

import fpl_optimizer_functions as fpl

# FPL API URL
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
response = requests.get(url)
json = response.json()



# storing json outputs as dataframes

elements_df = pd.DataFrame(json['elements'])
elements_types_df = pd.DataFrame(json['element_types'])
teams_df = pd.DataFrame(json['teams'])


# Pulling in player position into elements_df

elements_df['position'] = elements_df.element_type.map(elements_types_df.set_index('id').singular_name)
elements_df['team_name'] = elements_df.team.map(teams_df.set_index('id').name)



# Filtering out only the necessary columns

slim_elements_df = elements_df[['first_name', 'second_name','team_name','position','news','selected_by_percent','in_dreamteam',
                                'now_cost','form','points_per_game','minutes','goals_scored','assists','clean_sheets',
                                'goals_conceded','clean_sheets','goals_conceded','yellow_cards','red_cards','saves','bonus',
                                'transfers_in','value_season','total_points','influence','creativity','threat','ict_index']]

# numeric columns:

numeric_cols = ['selected_by_percent','form','points_per_game','value_season','influence','creativity','threat','ict_index']


# convering columns into numeric data type

for col in numeric_cols:
    slim_elements_df[col] = pd.to_numeric(slim_elements_df[col])



# actual cost of the player is now_cost/10
slim_elements_df['actual_cost'] = slim_elements_df['now_cost']/10

# creating additional metrics
slim_elements_df['games_completed'] = slim_elements_df['minutes']/90
slim_elements_df['points_per_90_mins'] = slim_elements_df['total_points']/slim_elements_df['games_completed']
slim_elements_df['ga_per_90_mins'] = (slim_elements_df['goals_scored']+slim_elements_df['assists'])/slim_elements_df['games_completed']
slim_elements_df['points_per_million'] = slim_elements_df['total_points']/slim_elements_df['actual_cost']

# eligible players
eligible_players = slim_elements_df[slim_elements_df['news'] == '']

# create a dataframe with only differentials: owned by less than 20%
differentials = slim_elements_df.loc[(slim_elements_df['news'] == '') & (slim_elements_df['selected_by_percent'] <= 20)]


# Final optimized squad
squad = fpl.squad_optimizer(eligible_players, 'total_points')


# Write dataframe into PostgreSql database

# establishing the connection
conn = psycopg2.connect(
    database='template1', user='postgres', password='postgres', host='localhost', port= '5432'
)

# Setting auto commit true
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()

# Delete the contents of the table and load the dataframe
cursor.execute('''DROP TABLE IF EXISTS fpl_squad''')

engine = create_engine('postgresql://postgres:postgres@localhost:5432/template1')
squad.to_sql('fpl_squad', engine)