# This airflow dag takes data from the FPL API, cleans it and runs it through the optimizer functions

from datetime import timedelta

import pandas as pd
import pandas.io.sql as sqlio
import pendulum
import psycopg2
from sqlalchemy import create_engine

import config
import fpl_optimizer_functions as fpl
import raw_players_data_upload as upload

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


################################ DAG arguments ################################

default_args = {
    'owner': 'Prathamesh',
    'email': ['prathamesh.murugesan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

################################# define the DAG #################################

fpl_etl = DAG(
    dag_id='fpl_etl_dag',
    default_args=default_args,
    description='Create optimal FPL squad and load into Supabase',
    schedule='0 */3 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    catchup=False,
)

################################# python function to extract FPL data #################################

def fpl_extract_and_clean():

    # fetch + clean the players data (shared logic lives in raw_players_data_upload)
    slim_elements_df = upload.fetch_players_df()

    # connect to Supabase and replace analytics_fpl.dim_fpl_players
    engine = upload.make_engine(
        config.supabase_fpl_username,
        config.supabase_fpl_password,
        config.supabase_fpl_host,
        config.supabase_fpl_port,
        config.supabase_fpl_db,
    )
    upload.load_players_df(slim_elements_df, engine)

################################# Task 1: Extract and load #################################

extract_and_load = PythonOperator(
    task_id='extract_and_load',
    python_callable=fpl_extract_and_clean,
    dag=fpl_etl,
)

################################# squad optimizer function #################################

def squad_optimizer():

    # connect to Supabase
    user = config.supabase_fpl_username
    password = config.supabase_fpl_password
    host = config.supabase_fpl_host
    port = config.supabase_fpl_port
    database = config.supabase_fpl_db

    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port
    )

    # Setting auto commit true
    conn.autocommit = True

    # save query output as dataframe
    sql = "select * from analytics_fpl.dim_fpl_players;"
    eligible_players = sqlio.read_sql_query(sql, conn)

    # get the optimized squad
    squad = fpl.squad_optimizer(eligible_players, 'total_points')

    # open a cursor
    cursor = conn.cursor()

    # metrics for which the squad is to be optimized
    optimizing_metrics = ['points_per_game','bonus','total_points','ict_index','points_per_million']

    # sqlalchemy engine
    engine_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + database
    engine = create_engine(engine_url)

    # Create individual tables for each metric
    for metric in optimizing_metrics:
        squad = fpl.squad_optimizer(eligible_players, metric)
        fpl.load_optimal_squad(squad, metric, engine)

################################# Task 2: Load optimized squad #################################

load_opt_squad = PythonOperator(
    task_id='load_opt_squad',
    python_callable=squad_optimizer,
    dag=fpl_etl,
)

################################# Task pipeline #################################

extract_and_load >> load_opt_squad
