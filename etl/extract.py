"""
etl/extract.py

PURPOSE: Extract layer of the ETL pipeline.
Reads all 5 raw CSV data sources, validates their schema,
enforces correct data types, and returns clean DataFrames
ready for the Transform phase.

DATA SOURCES HANDLED:
  1. train.csv          — Walmart weekly sales (primary fact)
  2. features.csv       — Store-level economic features
  3. stores.csv         — Store metadata (type, size)
  4. cpi_fred.csv       — US monthly CPI from FRED/BLS
  5. unemployment_fred.csv — US monthly unemployment from FRED/BLS
"""

import os
import hashlib
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

# Load .env file so os.getenv() can read our credentials and paths
load_dotenv()

# Path to raw data folder (read from .env)
RAW = os.getenv('DATA_RAW', 'data/raw')


# ─── Schema Contracts ────────────────────────────────────────────────────────
# We define exactly which columns each source MUST have.
# If columns are missing, the pipeline fails immediately with a clear error.
EXPECTED_COLS = {
    'train': ['Store', 'Dept', 'Date', 'Weekly_Sales', 'IsHoliday'],
    'features': [
        'Store', 'Date', 'Temperature', 'Fuel_Price',
        'MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5',
        'CPI', 'Unemployment', 'IsHoliday'
    ],
    'stores': ['Store', 'Type', 'Size'],
    'cpi':    ['observation_date', 'CPIAUCSL'],   # FRED uses observation_date
    'unemp':  ['observation_date', 'UNRATE'],     # FRED uses observation_date
}


# ─── Helper: Schema Validator ─────────────────────────────────────────────────
def _validate_schema(df: pd.DataFrame, name: str) -> None:
    """
    Check that all expected columns are present in the DataFrame.
    Raises ValueError with clear message if any columns are missing.
    """
    missing = set(EXPECTED_COLS[name]) - set(df.columns)
    if missing:
        raise ValueError(
            f'[EXTRACT][{name}] Schema mismatch — missing columns: {missing}'
        )
    logger.info(f'[EXTRACT][{name}] Schema OK — {len(df):,} rows, {len(df.columns)} columns')


# ─── Helper: File Checksum ────────────────────────────────────────────────────
def _log_checksum(path: str) -> None:
    """Log MD5 checksum of the file — proves data integrity in audit trail."""
    with open(path, 'rb') as f:
        checksum = hashlib.md5(f.read()).hexdigest()
    size_mb = os.path.getsize(path) / (1024 * 1024)
    logger.info(f'[EXTRACT] {os.path.basename(path)} — {size_mb:.2f} MB — MD5: {checksum}')


# ─── Extractor 1: Walmart Sales ──────────────────────────────────────────────
def extract_sales() -> pd.DataFrame:
    """
    Extract primary sales fact data from train.csv.

    Returns DataFrame with columns:
      Store (int), Dept (int), Date (datetime), Weekly_Sales (float), IsHoliday (bool)
    """
    path = os.path.join(RAW, 'train.csv')
    logger.info(f'[EXTRACT] Reading sales data: {path}')
    _log_checksum(path)

    df = pd.read_csv(
        path,
        dtype={
            'Store':     'int8',   # 45 stores — int8 saves memory
            'Dept':      'int8',   # max 99 depts — int8 is fine
            'IsHoliday': 'bool',
        },
        parse_dates=['Date'],      # Convert Date string to datetime
    )
    _validate_schema(df, 'train')

    # Convert Weekly_Sales to float32 (half the memory of float64)
    df['Weekly_Sales'] = df['Weekly_Sales'].astype('float32')
    return df


# ─── Extractor 2: Walmart Features ───────────────────────────────────────────
def extract_features() -> pd.DataFrame:
    """
    Extract store-level economic indicators from features.csv.

    Contains: Temperature, Fuel_Price, MarkDown1-5, CPI, Unemployment
    Note: MarkDown columns have many NaN values — handled in Transform.
    """
    path = os.path.join(RAW, 'features.csv')
    logger.info(f'[EXTRACT] Reading features data: {path}')
    _log_checksum(path)

    df = pd.read_csv(
        path,
        dtype={
            'Store':     'int8',
            'IsHoliday': 'bool',
        },
        parse_dates=['Date'],
        na_values=['NA', 'na', 'N/A', ''],  # Handle all null representations
    )
    _validate_schema(df, 'features')
    return df


# ─── Extractor 3: Walmart Store Metadata ─────────────────────────────────────
def extract_stores() -> pd.DataFrame:
    """
    Extract store metadata from stores.csv.
    45 rows: one per store. Has Type (A/B/C) and Size (sq footage).
    """
    path = os.path.join(RAW, 'stores.csv')
    logger.info(f'[EXTRACT] Reading stores data: {path}')
    _log_checksum(path)

    df = pd.read_csv(
        path,
        dtype={
            'Store': 'int8',
            'Type':  'str',
            'Size':  'int32',
        },
    )
    _validate_schema(df, 'stores')
    return df


# ─── Extractor 4: US CPI from FRED ───────────────────────────────────────────
def extract_cpi() -> pd.DataFrame:
    """
    Extract US Consumer Price Index from FRED download (cpi_fred.csv).
    FRED format: observation_date column (YYYY-MM-DD), CPIAUCSL column (float)
    Returns monthly CPI data as a clean DataFrame.
    """
    path = os.path.join(RAW, 'cpi_fred.csv')
    logger.info(f'[EXTRACT] Reading FRED CPI data: {path}')
    _log_checksum(path)

    df = pd.read_csv(
        path,
        parse_dates=['observation_date'],   # FRED uses observation_date
        na_values=['.'],                    # FRED uses '.' for missing values
    )
    _validate_schema(df, 'cpi')

    # Rename to standard internal names
    df = df.rename(columns={'observation_date': 'Date', 'CPIAUCSL': 'BLS_CPI'})
    df = df.dropna(subset=['BLS_CPI'])
    df['BLS_CPI'] = df['BLS_CPI'].astype('float32')

    logger.info(f'[EXTRACT] CPI data: {df.Date.min().date()} to {df.Date.max().date()}')
    return df


# ─── Extractor 5: US Unemployment Rate from FRED ─────────────────────────────
def extract_unemployment() -> pd.DataFrame:
    """
    Extract US Unemployment Rate from FRED download (unemployment_fred.csv).
    FRED format: observation_date column (YYYY-MM-DD), UNRATE column (float)
    """
    path = os.path.join(RAW, 'unemployment_fred.csv')
    logger.info(f'[EXTRACT] Reading FRED Unemployment data: {path}')
    _log_checksum(path)

    df = pd.read_csv(
        path,
        parse_dates=['observation_date'],   # FRED uses observation_date
        na_values=['.'],
    )
    _validate_schema(df, 'unemp')

    # Rename to standard internal names
    df = df.rename(columns={'observation_date': 'Date', 'UNRATE': 'National_Unemployment'})
    df = df.dropna(subset=['National_Unemployment'])
    df['National_Unemployment'] = df['National_Unemployment'].astype('float32')

    logger.info(f'[EXTRACT] Unemployment: {df.Date.min().date()} to {df.Date.max().date()}')
    return df


# ─── Master Extract Function ──────────────────────────────────────────────────
def extract_all() -> dict:
    """
    Run all 5 extractors and return a dictionary of DataFrames.
    This is the only function called from run_pipeline.py.

    Returns:
        dict with keys: 'sales', 'features', 'stores', 'cpi', 'unemployment'
    """
    logger.info('=' * 60)
    logger.info('[EXTRACT] PHASE START — reading 5 data sources')
    logger.info('=' * 60)

    data = {
        'sales':        extract_sales(),
        'features':     extract_features(),
        'stores':       extract_stores(),
        'cpi':          extract_cpi(),
        'unemployment': extract_unemployment(),
    }

    # Log summary of all sources
    total_rows = sum(len(v) for v in data.values())
    for name, df in data.items():
        logger.info(f'  {name:15s}: {len(df):>8,} rows, {len(df.columns)} columns')
    logger.success(f'[EXTRACT] PHASE COMPLETE — {total_rows:,} total rows extracted')

    return data