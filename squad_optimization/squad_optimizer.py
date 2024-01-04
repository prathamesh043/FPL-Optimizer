import requests
import pandas as pd
import numpy as np

import psycopg2
from sqlalchemy import create_engine

import fpl_optimizer_functions as fpl

# for env variables
import os
from dotenv import load_dotenv, get_key
load_dotenv()

# save env variables
SUPABASE_USER = get_key('.env', 'SUPABASE_USER')
SUPABASE_HOST = get_key('.env', 'SUPABASE_HOST')
SUPABASE_PASSWORD = get_key('.env', 'SUPABASE_PASSWORD')
SUPABASE_PORT = get_key('.env', 'SUPABASE_PORT')
SUPABASE_DB = get_key('.env', 'SUPABASE_DB')

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
slim_elements_df = elements_df[['id', 'first_name','second_name','web_name','team_name','position','news','selected_by_percent','in_dreamteam',
                                'now_cost','form','points_per_game','minutes','goals_scored','assists','clean_sheets',
                                'goals_conceded','yellow_cards','red_cards','saves','bonus',
                                'transfers_in','starts','value_season','total_points','influence','creativity','threat','ict_index']]

slim_elements_df.rename(columns = {'web_name':'name'}, inplace = True)

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
slim_elements_df['goal_contributions'] = (slim_elements_df['goals_scored']+slim_elements_df['assists'])
slim_elements_df['points_per_million'] = slim_elements_df['total_points']/slim_elements_df['actual_cost']

# eligible players
eligible_players = slim_elements_df[slim_elements_df['news'] == '']

# create a dataframe with only differentials: owned by less than 20%
differentials = slim_elements_df.loc[(slim_elements_df['news'] == '') & (slim_elements_df['selected_by_percent'] <= 20)]

print("Data processing done")


##################### Loading data into Supabase #####################


# establish connection
user=SUPABASE_USER
password=SUPABASE_PASSWORD
host=SUPABASE_HOST
port=SUPABASE_PORT
database=SUPABASE_DB

conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)

# Setting auto commit true
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()

optimizing_metrics = ['points_per_game','bonus','total_points','ict_index','points_per_million']

for metric in optimizing_metrics:

    table_name = 'public.optimal_squad_' + metric
    drop_query = 'DROP TABLE IF EXISTS ' + table_name
    cursor.execute(drop_query)

engine_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + database
engine = create_engine(engine_url)


for metric in optimizing_metrics:

    table_name = 'optimal_squad_' + metric
    squad = fpl.squad_optimizer(eligible_players, metric)
    squad = pd.merge(squad, slim_elements_df[['first_name', 'second_name', 'id']], on=['first_name', 'second_name'], how='left')
    squad.to_sql(table_name, engine, schema='public', index=False)


# Load raw FPL API data in another table

# Delete the contents of the table and load the dataframe
cursor.execute('''DROP TABLE IF EXISTS public.dim_fpl_players''')

# Use sqlalchemy engine to write to the DB

engine_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + database

engine = create_engine(engine_url)
slim_elements_df.to_sql('dim_fpl_players', engine, schema='public', index=False)

conn.close()

print("Data loaded to Supabase")