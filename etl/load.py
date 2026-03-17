"""
etl/load.py

PURPOSE: Load layer of the ETL pipeline.
Takes the final transformed DataFrame and loads it into
the PostgreSQL star schema using psycopg2 COPY for maximum speed.

LOAD ORDER (respects foreign key constraints):
  1. Run DDL (create_schema.sql) — create tables if not exist
  2. Load dim_store
  3. Load dim_date
  4. Load dim_dept
  5. Load dim_holiday
  6. Bulk load fact_sales (1M+ rows via COPY)
"""

import io
import os
import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


# ─── Database Connection ──────────────────────────────────────────────────────
def get_engine():
    """Create and return a SQLAlchemy engine using .env credentials."""
    user     = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host     = os.getenv('DB_HOST')
    port     = os.getenv('DB_PORT', '5432')
    dbname   = os.getenv('DB_NAME')
    url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    return create_engine(url, pool_pre_ping=True)


# ─── DDL Execution ────────────────────────────────────────────────────────────
def run_ddl(engine) -> None:
    """Execute create_schema.sql to create all tables if they do not exist."""
    ddl_path = os.path.join('sql', 'create_schema.sql')

    # encoding='utf-8' fixes Windows charmap codec error
    with open(ddl_path, 'r', encoding='utf-8') as f:
        ddl = f.read()

    with engine.connect() as conn:
        # Execute each statement separately (handles multi-statement SQL)
        for stmt in ddl.split(';'):
            stmt = stmt.strip()
            if stmt:  # Skip empty strings
                conn.execute(text(stmt))
        conn.commit()
    logger.info('[LOAD] Database schema created / verified')


# ─── Helper: Load Dimension Table ────────────────────────────────────────────
def _load_dimension(df: pd.DataFrame, table: str, engine) -> pd.DataFrame:
    """
    Load a dimension table and return the full table with surrogate keys.
    Uses to_sql with if_exists='append' — safe to re-run (no duplicates
    because of UNIQUE constraints on natural keys).
    """
    try:
        df.to_sql(
            table,
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        logger.info(f'[LOAD] {table}: {len(df)} rows inserted')
    except Exception as e:
        # Unique constraint violation on re-run — already loaded, that is OK
        logger.warning(f'[LOAD] {table}: insert skipped (likely already loaded): {e}')

    # Always return the full table with surrogate keys
    return pd.read_sql(f'SELECT * FROM {table}', engine)


# ─── Load All Dimension Tables ────────────────────────────────────────────────
def load_dimensions(df: pd.DataFrame, engine) -> dict:
    """
    Load all 4 dimension tables and return a dict of key mappings.
    The key mappings let us replace natural keys with surrogate keys
    in the fact table.
    """
    logger.info('[LOAD] Loading dimension tables...')
    keys = {}

    # ── dim_store ──────────────────────────────────────────────────────
    stores_df = df[['Store', 'Type', 'Size', 'size_bucket']].drop_duplicates()
    stores_df = stores_df.copy()
    stores_df.columns = ['store_id', 'store_type', 'store_size', 'size_bucket']
    dim_store = _load_dimension(stores_df, 'dim_store', engine)
    keys['store'] = dict(zip(dim_store['store_id'], dim_store['store_key']))

    # ── dim_date ───────────────────────────────────────────────────────
    dates_df = df[['Date', 'year', 'quarter', 'month', 'week_of_year', 'is_month_end']]
    dates_df = dates_df.drop_duplicates('Date').copy()
    dates_df.columns = ['full_date', 'year', 'quarter', 'month', 'week_of_year', 'is_month_end']
    dim_date = _load_dimension(dates_df, 'dim_date', engine)
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
    keys['date'] = dict(zip(dim_date['full_date'], dim_date['date_key']))

    # ── dim_dept ───────────────────────────────────────────────────────
    dept_df = df[['Dept']].drop_duplicates().copy()
    dept_df.columns = ['dept_id']
    dim_dept = _load_dimension(dept_df, 'dim_dept', engine)
    keys['dept'] = dict(zip(dim_dept['dept_id'], dim_dept['dept_key']))

    # ── dim_holiday ────────────────────────────────────────────────────
    holiday_df = df[['holiday_type']].drop_duplicates().copy()
    dim_hol = _load_dimension(holiday_df, 'dim_holiday', engine)
    keys['holiday'] = dict(zip(dim_hol['holiday_type'], dim_hol['holiday_key']))

    logger.success('[LOAD] All 4 dimension tables loaded')
    return keys


# ─── Bulk Load Fact Table ─────────────────────────────────────────────────────
def bulk_load_fact(df: pd.DataFrame, keys: dict, engine) -> None:
    """
    Build the fact_sales DataFrame with surrogate keys and bulk insert
    using psycopg2 COPY command — the fastest way to load large datasets
    into PostgreSQL. Much faster than INSERT for 1M+ rows.
    """
    logger.info(f'[LOAD] Preparing {len(df):,} rows for fact_sales...')

    df = df.copy()

    # Map natural keys to surrogate keys
    df['store_key']   = df['Store'].map(keys['store'])
    df['date_key']    = df['Date'].map(keys['date'])
    df['dept_key']    = df['Dept'].map(keys['dept'])
    df['holiday_key'] = df['holiday_type'].map(keys['holiday'])

    # Select only the columns needed for fact table (in correct order)
    fact_cols = [
        'store_key', 'date_key', 'dept_key', 'holiday_key',
        'Weekly_Sales', 'Temperature', 'Fuel_Price',
        'CPI', 'Unemployment',
        'BLS_CPI', 'National_Unemployment',
        'total_markdown', 'sales_4wk_avg', 'sales_vs_dept_avg',
        'cpi_normalized_sales', 'IsHoliday',
    ]
    fact = df[fact_cols].copy()
    fact.columns = [
        'store_key', 'date_key', 'dept_key', 'holiday_key',
        'weekly_sales', 'temperature', 'fuel_price',
        'cpi', 'unemployment',
        'bls_cpi', 'national_unemployment',
        'total_markdown', 'sales_4wk_avg', 'sales_vs_dept_avg',
        'cpi_normalized_sales', 'is_holiday',
    ]

    # Drop any rows with null surrogate keys (safety check)
    key_cols = ['store_key', 'date_key', 'dept_key', 'holiday_key']
    null_keys = fact[key_cols].isnull().any(axis=1).sum()
    if null_keys > 0:
        logger.warning(f'[LOAD] Dropping {null_keys} rows with unmapped keys')
        fact = fact.dropna(subset=key_cols)

    logger.info(f'[LOAD] Starting COPY bulk insert: {len(fact):,} rows...')

    # Use raw psycopg2 connection for COPY (bypasses SQLAlchemy overhead)
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()

        # Write DataFrame to in-memory CSV buffer
        # encoding handled via StringIO — no Windows charmap issues
        buffer = io.StringIO()
        fact.to_csv(buffer, index=False, header=False, na_rep='')
        buffer.seek(0)  # Rewind to start

        # COPY from buffer into fact_sales
        cols = ','.join(fact.columns)
        cursor.copy_expert(
            f"COPY fact_sales ({cols}) FROM STDIN WITH CSV NULL ''",
            buffer
        )
        raw_conn.commit()
        logger.success(f'[LOAD] fact_sales: {len(fact):,} rows loaded via COPY')

    except Exception as e:
        raw_conn.rollback()
        logger.error(f'[LOAD] COPY failed: {e}')
        raise

    finally:
        cursor.close()
        raw_conn.close()


# ─── Master Load Function ─────────────────────────────────────────────────────
def load_all(df: pd.DataFrame) -> None:
    """
    Orchestrate the complete load phase.
    Called from run_pipeline.py.
    """
    logger.info('=' * 60)
    logger.info('[LOAD] PHASE START')
    logger.info('=' * 60)

    engine = get_engine()
    run_ddl(engine)
    keys = load_dimensions(df, engine)
    bulk_load_fact(df, keys, engine)

    logger.success('[LOAD] PHASE COMPLETE')