# This airflow dag takes data from the FPL API, cleans it and runs it through the optimizer functions

import logging
import os
from datetime import timedelta

import pandas as pd
import pendulum

import config
import fpl_optimizer_functions as fpl
import raw_players_data_upload as upload
import raw_fixtures_data_upload as fixtures_upload
import raw_history_data_upload as history_upload

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

################################ DAG arguments ################################

default_args = {
    'owner': 'Prathamesh',
    'email': ['prathamesh.murugesan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
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

################################# Supabase connection #################################

def get_engine():
    """Each task runs in its own process, so every task builds its own engine."""
    return upload.make_engine(
        config.supabase_fpl_username,
        config.supabase_fpl_password,
        config.supabase_fpl_host,
        config.supabase_fpl_port,
        config.supabase_fpl_db,
    )

################################# python function to extract FPL data #################################

def fpl_extract_and_load():

    # fetch every table in the bootstrap-static response as-is (transformations happen in dbt)
    raw_tables = upload.fetch_raw_tables()

    # replace raw_fpl.<table> for each API table
    upload.load_raw_tables(raw_tables, get_engine())
    logger.info("Loaded raw tables to %s: %s", upload.RAW_SCHEMA, ', '.join(raw_tables))

################################# Task 1: Extract and load #################################

extract_and_load = PythonOperator(
    task_id='extract_and_load',
    python_callable=fpl_extract_and_load,
    dag=fpl_etl,
)

################################# Task: Load raw fixtures #################################

def fpl_fixtures_extract_and_load():
    fixtures_df = fixtures_upload.fetch_raw_fixtures()
    fixtures_upload.load_raw_fixtures(fixtures_df, get_engine())
    logger.info("Loaded %d fixtures to %s.%s", len(fixtures_df), fixtures_upload.RAW_SCHEMA, fixtures_upload.FIXTURES_TABLE)

extract_and_load_fixtures = PythonOperator(
    task_id='extract_and_load_fixtures',
    python_callable=fpl_fixtures_extract_and_load,
    dag=fpl_etl,
)

################################# Task: Load raw player history #################################

def fpl_history_extract_and_load():
    history_df = history_upload.fetch_raw_history()
    history_upload.load_raw_history(history_df, get_engine())
    logger.info("Loaded %d history rows to %s.%s", len(history_df), history_upload.RAW_SCHEMA, history_upload.HISTORY_TABLE)

extract_and_load_history = PythonOperator(
    task_id='extract_and_load_history',
    python_callable=fpl_history_extract_and_load,
    dag=fpl_etl,
)

################################# dbt models #################################

DBT_PROJECT_DIR = os.environ.get('DBT_PROJECT_DIR', '/opt/airflow/fpl_dbt')

dbt_run_all = BashOperator(
    task_id='dbt_run_all',
    bash_command=f'dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}',
    dag=fpl_etl,
)

################################# squad optimizer function #################################

def squad_optimizer():
    engine = get_engine()

    # save query output as dataframe
    eligible_players = pd.read_sql('select * from analytics_fpl.dim_fpl_players;', engine)

    # metrics for which the squad is to be optimized
    optimizing_metrics = ['points_per_game','bonus','total_points','ict_index','form']

    # Create individual tables for each metric
    for metric in optimizing_metrics:
        squad = fpl.squad_optimizer(eligible_players, metric)
        fpl.load_optimal_squad(squad, metric, engine)
        logger.info("Loaded optimal squad for metric '%s' to Supabase", metric)

################################# Task 2: Load optimized squad #################################

load_opt_squad = PythonOperator(
    task_id='load_opt_squad',
    python_callable=squad_optimizer,
    dag=fpl_etl,
)

################################# Task pipeline #################################

[extract_and_load, extract_and_load_fixtures, extract_and_load_history] >> dbt_run_all >> load_opt_squad
