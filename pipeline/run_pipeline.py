"""Run the FPL pipeline outside Airflow (used by the GitHub Actions workflow)."""

import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

import config
import fpl_optimizer_functions as fpl
import raw_fixtures_data_upload as fixtures_upload
import raw_history_data_upload as history_upload
import raw_players_data_upload as upload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPTIMIZING_METRICS = ['points_per_game', 'bonus', 'total_points', 'ict_index', 'form']


def load_raw(engine):
    raw_tables = upload.fetch_raw_tables()
    upload.load_raw_tables(raw_tables, engine)
    logger.info("Loaded raw tables: %s", ', '.join(raw_tables))


def load_fixtures(engine):
    fixtures_df = fixtures_upload.fetch_raw_fixtures()
    fixtures_upload.load_raw_fixtures(fixtures_df, engine)
    logger.info("Loaded %d fixtures", len(fixtures_df))


def load_history(engine):
    history_df = history_upload.fetch_raw_history()
    history_upload.load_raw_history(history_df, engine)
    logger.info("Loaded %d history rows", len(history_df))


def main():
    engine = upload.make_engine(
        config.supabase_fpl_username,
        config.supabase_fpl_password,
        config.supabase_fpl_host,
        config.supabase_fpl_port,
        config.supabase_fpl_db,
    )

    with ThreadPoolExecutor() as pool:
        futures = [pool.submit(load, engine) for load in (load_raw, load_fixtures, load_history)]
        for future in futures:
            future.result()

    subprocess.run(
        ['dbt', 'run', '--project-dir', 'fpl_dbt', '--profiles-dir', 'fpl_dbt'],
        check=True,
    )

    eligible_players = pd.read_sql('select * from analytics_fpl.dim_fpl_players;', engine)
    for metric in OPTIMIZING_METRICS:
        squad = fpl.squad_optimizer(eligible_players, metric)
        fpl.load_optimal_squad(squad, metric, engine)
        logger.info("Loaded optimal squad for metric '%s'", metric)


if __name__ == '__main__':
    main()