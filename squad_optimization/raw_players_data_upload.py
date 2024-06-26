import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

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

def convert_filename(string):
    return string.lower().replace(" ", "_")


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

# path to team image icon
slim_elements_df['image_path'] = '/' + slim_elements_df['team_name'].apply(convert_filename) + '.svg'

# create percentile columns for specific metrics
percentile_metrics = ['bonus', 'form', 'ict_index', 'points_per_game', 'points_per_million', 'total_points', 'goals_scored', 'assists', 'clean_sheets']

for metric in percentile_metrics:
    slim_elements_df[metric + '_percentile'] = slim_elements_df.groupby('position')[metric].rank(pct=True)

# Connect to Supabase
conn = psycopg2.connect(
    database=SUPABASE_DB, user=SUPABASE_USER, password=SUPABASE_PASSWORD, host=SUPABASE_HOST, port=SUPABASE_PORT
)

# Setting auto commit true
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()

# drop existing players table
drop_query = 'DROP TABLE IF EXISTS public.dim_fpl_players;'
cursor.execute(drop_query)

# create SQL alchemy engine
engine_url = 'postgresql://' + SUPABASE_USER + ':' + SUPABASE_PASSWORD + '@' + SUPABASE_HOST + '/' + SUPABASE_DB
engine = create_engine(engine_url)

# upload to Supabase
slim_elements_df.to_sql('dim_fpl_players', engine, schema='public', index=False)

conn.close()

print("Data loaded to Supabase")