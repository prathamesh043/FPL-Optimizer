import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

FPL_API_URL = 'https://fantasy.premierleague.com/api/bootstrap-static/'
PLAYERS_SCHEMA = 'analytics_fpl'
PLAYERS_TABLE = 'dim_fpl_players'

COLUMNS = [
    'id', 'first_name', 'second_name', 'web_name', 'team_name', 'team_short_name', 'position',
    'news', 'selected_by_percent', 'in_dreamteam', 'now_cost', 'form', 'points_per_game',
    'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded', 'yellow_cards',
    'red_cards', 'saves', 'bonus', 'transfers_in', 'starts', 'value_season', 'total_points',
    'influence', 'creativity', 'threat', 'ict_index',
]

RAW_SCHEMA = 'raw_fpl'

NUMERIC_COLS = ['selected_by_percent', 'form', 'points_per_game', 'value_season',
                'influence', 'creativity', 'threat', 'ict_index']

PERCENTILE_METRICS = ['bonus', 'form', 'ict_index', 'points_per_game', 'points_per_million',
                      'total_points', 'goals_scored', 'assists', 'clean_sheets']


def convert_filename(string):
    return string.lower().replace(" ", "_")


def fetch_players_df():
    """Pull bootstrap-static from the FPL API and return the cleaned players DataFrame."""
    data = requests.get(FPL_API_URL).json()

    elements_df = pd.DataFrame(data['elements'])
    elements_types_df = pd.DataFrame(data['element_types'])
    teams_df = pd.DataFrame(data['teams'])

    # Pulling in player position and team info into elements_df
    elements_df['position'] = elements_df.element_type.map(elements_types_df.set_index('id').singular_name)
    elements_df['team_name'] = elements_df.team.map(teams_df.set_index('id').name)
    elements_df['team_short_name'] = elements_df.team.map(teams_df.set_index('id').short_name)

    df = elements_df[COLUMNS].rename(columns={'web_name': 'name'})

    # converting columns into numeric data type
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col])

    # actual cost of the player is now_cost/10
    df['actual_cost'] = df['now_cost'] / 10

    # additional metrics
    df['games_completed'] = df['minutes'] / 90
    df['points_per_90_mins'] = df['total_points'] / df['games_completed']
    df['ga_per_90_mins'] = (df['goals_scored'] + df['assists']) / df['games_completed']
    df['goal_contributions'] = df['goals_scored'] + df['assists']
    df['points_per_million'] = df['total_points'] / df['actual_cost']

    # path to team image icon
    df['image_path'] = '/' + df['team_name'].apply(convert_filename) + '.svg'

    # percentile columns for specific metrics
    for metric in PERCENTILE_METRICS:
        df[metric + '_percentile'] = df.groupby('position')[metric].rank(pct=True)

    return df


def _typecast(df):
    """Cast numeric strings to numbers; nested lists/dicts are stored as JSONB."""
    dtypes = {}
    for col in df.columns:
        values = df[col].dropna()
        if values.map(lambda v: isinstance(v, (list, dict))).any():
            dtypes[col] = JSONB()
        elif values.map(lambda v: isinstance(v, str)).all() and not values.empty:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass
    return df, dtypes


def fetch_raw_tables():
    """Pull bootstrap-static and return {api_key: DataFrame} for every list in the response, untransformed."""
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    data = response.json()
    return {key: pd.DataFrame(value) for key, value in data.items() if isinstance(value, list)}


def load_raw_tables(tables, engine):
    """Load each DataFrame into a staging table, then swap it into raw_fpl.<api_key>.

    Everything runs in one transaction: the target table is never dropped, and a
    failure rolls back (including the staging tables), leaving existing data intact.
    """
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}'))
        for name, df in tables.items():
            df, dtypes = _typecast(df)
            staging = f'{name}_staging'
            target = f'{RAW_SCHEMA}.{name}'
            stage = f'{RAW_SCHEMA}.{staging}'

            conn.execute(text(f'DROP TABLE IF EXISTS {stage}'))
            df.to_sql(staging, conn, schema=RAW_SCHEMA, index=False, dtype=dtypes)

            exists = conn.execute(text('SELECT to_regclass(:t)'), {'t': target}).scalar()
            if exists:
                cols = ', '.join(f'"{c}"' for c in df.columns)
                conn.execute(text(f'TRUNCATE TABLE {target}'))
                conn.execute(text(f'INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}'))
                conn.execute(text(f'DROP TABLE {stage}'))
            else:
                conn.execute(text(f'ALTER TABLE {stage} RENAME TO {name}'))


def make_engine(user, password, host, port, database):
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


def load_players_df(df, engine):
    """Replace analytics_fpl.dim_fpl_players with the given DataFrame."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {PLAYERS_SCHEMA}.{PLAYERS_TABLE}'))
    df.to_sql(PLAYERS_TABLE, engine, schema=PLAYERS_SCHEMA, index=False)


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
    load_players_df(fetch_players_df(), engine)
    print("Data loaded to Supabase")
