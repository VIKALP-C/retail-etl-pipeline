"""
etl/transform.py

PURPOSE: Transform layer of the ETL pipeline.
Takes the 5 raw DataFrames from extract_all() and:
  1. Cleans each source (nulls, types, negatives, duplicates)
  2. Merges all 5 sources on Store + Date keys
  3. Engineers 10 new analytical features
  4. Expands the dataset to 1M+ rows (synthetic scale-up)
"""

import numpy as np
import pandas as pd
from loguru import logger


# ─── Holiday Reference Table ──────────────────────────────────────────────────
# These are the 4 holiday weeks recognized by the Walmart competition.
# We use these to create a 'holiday_type' named column.
HOLIDAY_DATES = {
    'Super_Bowl':   ['2010-02-12', '2011-02-11', '2012-02-10', '2013-02-08'],
    'Labor_Day':    ['2010-09-10', '2011-09-09', '2012-09-07', '2013-09-06'],
    'Thanksgiving': ['2010-11-26', '2011-11-25', '2012-11-23', '2013-11-29'],
    'Christmas':    ['2010-12-31', '2011-12-30', '2012-12-28', '2013-12-27'],
}


# ═══════════════════════════════════════════════════════════════════════
# STEP 1: CLEAN FUNCTIONS (one per source)
# ═══════════════════════════════════════════════════════════════════════

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw sales DataFrame.
    Removes:
      - Duplicate Store+Dept+Date rows (data anomalies)
      - Negative Weekly_Sales (product returns, not real sales)
    Fixes:
      - Date type enforcement
      - Weekly_Sales to float32
    """
    logger.info('[TRANSFORM] Cleaning sales data...')
    original_len = len(df)
    df = df.copy()

    # Remove exact duplicates on natural key
    df = df.drop_duplicates(subset=['Store', 'Dept', 'Date'])
    dupes_removed = original_len - len(df)

    # Remove returns (negative weekly sales)
    df = df[df['Weekly_Sales'] >= 0]
    negatives_removed = (original_len - dupes_removed) - len(df)

    # Enforce types
    df['Date']         = pd.to_datetime(df['Date'])
    df['Weekly_Sales'] = df['Weekly_Sales'].astype('float32')
    df['IsHoliday']    = df['IsHoliday'].astype(bool)

    logger.info(
        f'[TRANSFORM] Sales cleaned: removed {dupes_removed} dupes, '
        f'{negatives_removed} negatives. Remaining: {len(df):,} rows'
    )
    return df


def clean_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the features DataFrame.
    Strategy for missing values:
      - MarkDown1-5: fill NaN with 0 (no markdown = no promotion active)
      - CPI/Unemployment: forward-fill within each Store group
        (carry last known value forward — standard economic data practice)
    """
    logger.info('[TRANSFORM] Cleaning features data...')
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])

    # MarkDown: NaN means no promotion was running — treat as 0
    md_cols = ['MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5']
    null_before = df[md_cols].isnull().sum().sum()
    df[md_cols] = df[md_cols].fillna(0.0)
    logger.info(f'[TRANSFORM] MarkDown nulls filled: {null_before:,} → 0')

    # CPI and Unemployment: forward fill within each store
    df = df.sort_values(['Store', 'Date'])
    for col in ['CPI', 'Unemployment']:
        null_count = df[col].isnull().sum()
        df[col] = df.groupby('Store')[col].transform(
            lambda x: x.ffill().bfill()
        )
        logger.info(f'[TRANSFORM] {col}: filled {null_count} nulls via forward-fill')

    return df


def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean store metadata.
    Strip whitespace from Type column, validate only A/B/C types exist.
    """
    logger.info('[TRANSFORM] Cleaning stores data...')
    df = df.copy()
    df['Type'] = df['Type'].str.strip().str.upper()

    # Validate store types
    valid_types = {'A', 'B', 'C'}
    invalid = set(df['Type'].unique()) - valid_types
    if invalid:
        logger.warning(f'[TRANSFORM] Unexpected store types found: {invalid}')

    # Add human-readable size bucket for analytics
    df['size_bucket'] = pd.cut(
        df['Size'],
        bins=[0, 100_000, 150_000, 999_999],
        labels=['Small', 'Medium', 'Large'],
    ).astype(str)

    logger.info(f'[TRANSFORM] Store types: {df["Type"].value_counts().to_dict()}')
    return df


# ═══════════════════════════════════════════════════════════════════════
# STEP 2: MERGE ALL 5 SOURCES
# ═══════════════════════════════════════════════════════════════════════

def merge_all_sources(
    sales: pd.DataFrame,
    features: pd.DataFrame,
    stores: pd.DataFrame,
    cpi: pd.DataFrame,
    unemployment: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge all 5 DataFrames into one master analytical DataFrame.

    Join strategy:
      sales + features  → LEFT JOIN on Store + Date + IsHoliday
      + stores          → LEFT JOIN on Store
      + cpi             → LEFT JOIN on year-month
      + unemployment    → LEFT JOIN on year-month
    """
    logger.info('[TRANSFORM] Merging all 5 data sources...')

    # ── Join 1: Sales + Features (store-level weekly data) ──
    df = sales.merge(
        features,
        on=['Store', 'Date', 'IsHoliday'],
        how='left',
    )
    logger.info(f'[TRANSFORM] After sales+features merge: {len(df):,} rows')

    # ── Join 2: + Store metadata ──
    df = df.merge(stores, on='Store', how='left')
    logger.info(f'[TRANSFORM] After adding stores: {len(df):,} rows')

    # ── Join 3 & 4: CPI and Unemployment on year-month ──
    # Economic data is monthly; sales are weekly.
    # We match by creating a year_month period key.
    df['year_month']           = df['Date'].dt.to_period('M')
    cpi['year_month']          = cpi['Date'].dt.to_period('M')
    unemployment['year_month'] = unemployment['Date'].dt.to_period('M')

    df = df.merge(cpi[['year_month', 'BLS_CPI']], on='year_month', how='left')
    df = df.merge(
        unemployment[['year_month', 'National_Unemployment']],
        on='year_month',
        how='left'
    )
    df = df.drop(columns='year_month')

    logger.info(f'[TRANSFORM] Final merged shape: {df.shape} ({len(df):,} rows, {len(df.columns)} cols)')
    return df


# ═══════════════════════════════════════════════════════════════════════
# STEP 3: FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════════════

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 10 new analytical columns to the merged DataFrame.

    New columns created:
      Temporal:    year, quarter, month, week_of_year, is_month_end
      Holiday:     holiday_type (named holiday or 'None')
      Sales Lag:   sales_4wk_avg (4-week rolling average, lagged)
      Comparison:  sales_vs_dept_avg (% deviation from dept average)
      Economic:    cpi_normalized_sales (inflation-adjusted sales)
      Markdown:    total_markdown (sum of MarkDown1-5)
    """
    logger.info('[TRANSFORM] Engineering new analytical features...')
    df = df.copy()

    # ── Temporal Features ──────────────────────────────────────────────
    df['year']         = df['Date'].dt.year.astype('int16')
    df['quarter']      = df['Date'].dt.quarter.astype('int8')
    df['month']        = df['Date'].dt.month.astype('int8')
    df['week_of_year'] = df['Date'].dt.isocalendar().week.astype('int8')
    df['is_month_end'] = df['Date'].dt.is_month_end

    # ── Named Holiday Type ─────────────────────────────────────────────
    # Convert dates to strings for fast lookup against our holiday table
    date_str = df['Date'].dt.strftime('%Y-%m-%d')
    df['holiday_type'] = 'None'
    for holiday_name, date_list in HOLIDAY_DATES.items():
        mask = date_str.isin(date_list)
        df.loc[mask, 'holiday_type'] = holiday_name
    logger.info(f'[TRANSFORM] Holiday distribution: {df["holiday_type"].value_counts().to_dict()}')

    # ── 4-Week Rolling Sales Average (lagged by 1 week) ────────────────
    # We shift(1) so we don't include the current week's sales in the average.
    # This is a standard forecasting feature that prevents data leakage.
    df = df.sort_values(['Store', 'Dept', 'Date'])
    df['sales_4wk_avg'] = (
        df.groupby(['Store', 'Dept'])['Weekly_Sales']
          .transform(lambda x: x.shift(1).rolling(window=4, min_periods=1).mean())
          .astype('float32')
    )

    # ── Sales vs Department Average ────────────────────────────────────
    # What percentage above or below the dept average is each week?
    dept_avg = df.groupby('Dept')['Weekly_Sales'].transform('mean')
    df['sales_vs_dept_avg'] = (
        (df['Weekly_Sales'] - dept_avg) / dept_avg.replace(0, np.nan)
    ).fillna(0).astype('float32')

    # ── CPI-Normalized Sales ───────────────────────────────────────────
    # Adjusts weekly sales for inflation using the internal CPI column.
    # Base index = 100. Lets us compare real purchasing power across years.
    base_cpi = df['CPI'].median()
    df['cpi_normalized_sales'] = (
        (df['Weekly_Sales'] / df['CPI'].replace(0, np.nan)) * base_cpi
    ).fillna(df['Weekly_Sales']).astype('float32')

    # ── Total Markdown ─────────────────────────────────────────────────
    # Sum all 5 markdown columns into one total promotional spend column
    md_cols = ['MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5']
    df['total_markdown'] = df[md_cols].sum(axis=1).astype('float32')

    logger.info(f'[TRANSFORM] Feature engineering done. Total columns: {len(df.columns)}')
    return df


# ═══════════════════════════════════════════════════════════════════════
# STEP 4: SYNTHETIC SCALE-UP TO 1M+ ROWS
# ═══════════════════════════════════════════════════════════════════════

def scale_to_million(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand the ~420K row dataset to 1M+ rows by simulating
    3 additional years of sales data (2013, 2014, 2015).

    METHOD: Pattern-preserving synthetic resampling.
    - Shift dates forward by N years
    - Add realistic Gaussian noise (mean=1.0, std=0.05) to sales
    - Apply gentle CPI drift (2% per year — realistic inflation)
    - Apply gentle fuel price drift (3% per year)

    This is a standard data engineering technique for pipeline
    scale testing. Clearly documented in the README.
    """
    logger.info('[TRANSFORM] Scaling dataset to 1M+ rows...')
    original = df.copy()
    np.random.seed(42)  # Reproducibility — same result every run

    synthetic_chunks = []
    for year_offset in [3, 4, 5]:  # Simulate years 2013, 2014, 2015
        chunk = original.copy()

        # Shift dates forward
        chunk['Date'] = chunk['Date'] + pd.DateOffset(years=year_offset)
        chunk['year'] = chunk['Date'].dt.year.astype('int16')

        # Sales: add ±5% random noise to preserve seasonality
        sales_noise = np.random.normal(loc=1.0, scale=0.05, size=len(chunk))
        chunk['Weekly_Sales'] = (chunk['Weekly_Sales'] * sales_noise).clip(lower=0)

        # CPI: drift upward by 2% per year (realistic US inflation)
        chunk['CPI']     = chunk['CPI']     * (1.02 ** year_offset)
        chunk['BLS_CPI'] = chunk['BLS_CPI'] * (1.02 ** year_offset)

        # Fuel: slight price drift
        chunk['Fuel_Price'] = chunk['Fuel_Price'] * (1.03 ** year_offset)

        # Recalculate CPI-normalized sales with new CPI
        base_cpi = chunk['CPI'].median()
        chunk['cpi_normalized_sales'] = (
            (chunk['Weekly_Sales'] / chunk['CPI'].replace(0, np.nan)) * base_cpi
        ).fillna(chunk['Weekly_Sales']).astype('float32')

        synthetic_chunks.append(chunk)
        logger.info(f'[TRANSFORM] Year +{year_offset}: {len(chunk):,} rows simulated')

    expanded = pd.concat([original] + synthetic_chunks, ignore_index=True)
    expanded = expanded.sort_values(['Store', 'Dept', 'Date']).reset_index(drop=True)

    logger.success(f'[TRANSFORM] Dataset expanded: {len(original):,} → {len(expanded):,} rows')
    return expanded


# ═══════════════════════════════════════════════════════════════════════
# MASTER TRANSFORM FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def transform_all(raw: dict) -> pd.DataFrame:
    """
    Orchestrate all transform steps in sequence.
    Called from run_pipeline.py.

    Args:
        raw: dict returned by extract_all()

    Returns:
        Single merged, cleaned, feature-engineered DataFrame with 1M+ rows
    """
    logger.info('=' * 60)
    logger.info('[TRANSFORM] PHASE START')
    logger.info('=' * 60)

    # Step 1: Clean each source
    sales        = clean_sales(raw['sales'])
    features     = clean_features(raw['features'])
    stores       = clean_stores(raw['stores'])
    cpi          = raw['cpi']           # Already clean from extract
    unemployment = raw['unemployment']  # Already clean from extract

    # Step 2: Merge all 5 sources
    merged = merge_all_sources(sales, features, stores, cpi, unemployment)

    # Step 3: Engineer analytical features
    featured = engineer_features(merged)

    # Step 4: Scale to 1M+ rows
    final = scale_to_million(featured)

    logger.success(f'[TRANSFORM] PHASE COMPLETE — {len(final):,} rows, {len(final.columns)} columns')
    return final