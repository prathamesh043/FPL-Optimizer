# Standard libraries
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2

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

# FPL API for fixtures
url = 'https://fantasy.premierleague.com/api/fixtures/'
response = requests.get(url)
fixtures_json = response.json()

# store in pandas DF
fixtures_df = pd.DataFrame(fixtures_json)

# FPL API for teams
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
response = requests.get(url)
json = response.json()

# storing json outputs as dataframes
teams_df = pd.DataFrame(json['teams'])

# getting team names into fixtures table
fixtures_df['home_team'] = pd.merge(fixtures_df, teams_df, left_on='team_h', right_on='id', how='left')['name']
fixtures_df['away_team'] = pd.merge(fixtures_df, teams_df, left_on='team_a', right_on='id', how='left')['name']

# removing unnecessary columns
fixtures_df = fixtures_df[['id', 'event', 'finished', 'kickoff_time', 'team_a', 'team_a_score', 'team_h', 'team_h_score',
                           'team_h_difficulty', 'team_a_difficulty', 'home_team', 'away_team']]

# convert kickoff time to timestamp
fixtures_df['kickoff_time'] = pd.to_datetime(fixtures_df['kickoff_time'])

# rename columns
fixtures_df = fixtures_df.rename(columns={'id': 'match_id', 'event': 'gameweek', 'team_a': 'away_team_id', 'team_h': 'home_team_id'})

# Load into Supabase

# establish connection
conn = psycopg2.connect(
    database=SUPABASE_DB, 
    user=SUPABASE_USER, 
    password=SUPABASE_PASSWORD, 
    host=SUPABASE_HOST, 
    port=SUPABASE_PORT
)

# Setting auto commit true
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()

# Drop the table before creating it again
drop_query = 'DROP TABLE IF EXISTS public.dim_fpl_fixtures;'
cursor.execute(drop_query)

# close the cursor and connection
cursor.close()
conn.close()

# Create SQL alchemy engine
engine_url = 'postgresql://' + SUPABASE_USER + ':' + SUPABASE_PASSWORD + '@' + SUPABASE_HOST + '/' + SUPABASE_DB
engine = create_engine(engine_url)

# Load the table in supabase
fixtures_df.to_sql(
    'dim_fpl_fixtures',
    engine,
    schema='public',
    index=False
)

print('Data uploaded to Supabase')