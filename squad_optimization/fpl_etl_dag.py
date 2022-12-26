# This airflow dag takes data from the FPL API, cleans it and runs it through the optimizer functions


import requests
import pandas as pd
import numpy as np
import copy
import sys

import config
import fpl_optimizer_functions as fpl

import psycopg2
from sqlalchemy import create_engine
import pandas.io.sql as sqlio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta


################################ DAG arguments ################################

default_args = {
    'owner': 'Prathamesh',
    'start_date': days_ago(0),
    'email': ['prathamesh.murugesan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

################################# define the DAG #################################

fpl_etl = DAG(
    'fpl_etl_dag',
    default_args=default_args,
    description='Create optimal FPL squad and load into Supabase',
    schedule='0 */3 * * *',
)

################################# python function to extract FPL data #################################

def fpl_extract_and_clean():

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
    slim_elements_df = elements_df[['id','first_name','second_name','team_name','position','news','selected_by_percent','in_dreamteam',
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
    
    # connect to Supabase
    user=config.supabase_fpl_username
    password=config.supabase_fpl_password
    host='db.pdqpnebestkagqneooty.supabase.co'
    port='5432'
    database='postgres'

    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port
    )

    # Setting auto commit true
    conn.autocommit = True

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    
    # Delete the contents of the table and load the dataframe
    cursor.execute('''DROP TABLE IF EXISTS raw_fpl.dim_fpl_players''')

    # Use sqlalchemy engine to write to the DB
    engine_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + database
    engine = create_engine(engine_url)
    slim_elements_df.to_sql('dim_fpl_players', engine, schema='raw_fpl', index=False)

    # close the connection and the cursor
    cursor.close()
    conn.close()

################################# Task 1: Extract and load #################################

extract_and_load = PythonOperator(task_id='extract_and_load', python_callable=fpl_extract_and_clean, dag=fpl_etl)

################################# squad optimizer function #################################    

def squad_optimizer():
    
    # connect to Supabase
    user=config.supabase_fpl_username
    password=config.supabase_fpl_password
    host='db.pdqpnebestkagqneooty.supabase.co'
    port='5432'
    database='postgres'

    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port
    )

    # Setting auto commit true
    conn.autocommit = True

    # save query output as dataframe
    sql = "select * from raw_fpl.dim_fpl_players;"
    eligible_players = sqlio.read_sql_query(sql, conn)

    # get the optimized squad
    squad = fpl.squad_optimizer(eligible_players, 'total_points')

    # Delete the contents of the table and load the dataframe
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS optsquads.fpl_squad''')

    # Use sqlalchemy engine to write to the DB
    engine_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + database
    engine = create_engine(engine_url)
    squad.to_sql('fpl_squad', engine, schema='optsquads', index=False)

################################# Task 2: Load optimized squad #################################

load_opt_squad = PythonOperator(task_id='load_opt_squad', python_callable=squad_optimizer, dag=fpl_etl)

################################# Task pipeline #################################

extract_and_load >> load_opt_squad