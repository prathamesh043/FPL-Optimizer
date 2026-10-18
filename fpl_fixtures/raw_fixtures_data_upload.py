import requests
import pandas as pd
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, MetaData, Table, Text, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

FIXTURES_API_URL = 'https://fantasy.premierleague.com/api/fixtures/'
RAW_SCHEMA = 'raw_fpl'
FIXTURES_TABLE = 'fixtures'


def fetch_raw_fixtures():
    """Pull fixtures from the FPL API and return them untransformed as a DataFrame."""
    response = requests.get(FIXTURES_API_URL)
    response.raise_for_status()
    return pd.DataFrame(response.json())


def _sql_type(series, dtypes):
    if series.name in dtypes:
        return dtypes[series.name]
    if pd.api.types.is_bool_dtype(series):
        return Boolean
    if pd.api.types.is_integer_dtype(series):
        return BigInteger
    if pd.api.types.is_float_dtype(series):
        return Float
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime
    return Text


def create_and_insert(conn, df, name, schema, dtypes):
    """Create schema.name from the DataFrame and insert its rows.

    Replaces DataFrame.to_sql, which needs SQLAlchemy 2.x (Airflow 3.0 pins 1.4).
    """
    table = Table(name, MetaData(), *[Column(c, _sql_type(df[c], dtypes)) for c in df.columns], schema=schema)
    table.create(conn)
    records = df.astype(object).where(df.notna(), None).to_dict('records')
    if records:
        conn.execute(table.insert(), records)


def load_raw_fixtures(df, engine):
    """Load fixtures into a staging table, then swap into raw_fpl.fixtures in one transaction.

    The target table is never dropped; a failure rolls back and leaves existing data intact.
    """
    # nested lists (e.g. stats) are stored as JSONB
    dtypes = {
        col: JSONB for col in df.columns
        if df[col].dropna().map(lambda v: isinstance(v, (list, dict))).any()
    }
    staging = f'{FIXTURES_TABLE}_staging'
    target = f'{RAW_SCHEMA}.{FIXTURES_TABLE}'
    stage = f'{RAW_SCHEMA}.{staging}'

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}'))
        conn.execute(text(f'DROP TABLE IF EXISTS {stage}'))
        create_and_insert(conn, df, staging, RAW_SCHEMA, dtypes)

        exists = conn.execute(text('SELECT to_regclass(:t)'), {'t': target}).scalar()
        if exists:
            cols = ', '.join(f'"{c}"' for c in df.columns)
            conn.execute(text(f'TRUNCATE TABLE {target}'))
            conn.execute(text(f'INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}'))
            conn.execute(text(f'DROP TABLE {stage}'))
        else:
            conn.execute(text(f'ALTER TABLE {stage} RENAME TO {FIXTURES_TABLE}'))


def make_engine(user, password, host, port, database):
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


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
    load_raw_fixtures(fetch_raw_fixtures(), engine)
    print("Fixtures loaded to Supabase")
