from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd

from raw_players_data_upload import FPL_API_URL, RAW_SCHEMA, load_raw_tables, make_engine

ELEMENT_SUMMARY_URL = 'https://fantasy.premierleague.com/api/element-summary/{}/'
HISTORY_TABLE = 'history'
MAX_WORKERS = 8


def fetch_player_ids(session):
    response = session.get(FPL_API_URL)
    response.raise_for_status()
    return [player['id'] for player in response.json()['elements']]


def fetch_player_history(session, player_id):
    """Return the player's per-match history for the current season."""
    response = session.get(ELEMENT_SUMMARY_URL.format(player_id))
    response.raise_for_status()
    return response.json()['history']


def fetch_raw_history():
    """Pull element-summary for every player and return all history rows untransformed as a DataFrame."""
    with requests.Session() as session:
        player_ids = fetch_player_ids(session)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            histories = pool.map(lambda pid: fetch_player_history(session, pid), player_ids)
            rows = [row for history in histories for row in history]
    return pd.DataFrame(rows)


def load_raw_history(df, engine):
    """Replace raw_fpl.history with the given DataFrame."""
    load_raw_tables({HISTORY_TABLE: df}, engine)


if __name__ == '__main__':
    # Manual run: read credentials from .env
    from dotenv import load_dotenv, get_key
    load_dotenv()

    engine = make_engine(
        get_key('.env', 'SUPABASE_USER'),
        get_key('.env', 'SUPABASE_PASSWORD'),
        get_key('.env', 'SUPABASE_HOST'),
        get_key('.env', 'SUPABASE_PORT') or '5432',
        get_key('.env', 'SUPABASE_DB'),
    )
    df = fetch_raw_history()
    load_raw_history(df, engine)
    print(f"Loaded {len(df)} history rows to {RAW_SCHEMA}.{HISTORY_TABLE}")
