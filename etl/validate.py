"""
etl/validate.py

PURPOSE: Data quality validation layer.
Runs assertions on the transformed DataFrame before loading.
Pipeline STOPS if any check fails — prevents bad data from reaching the DB.

CHECKS PERFORMED:
  1. Row count must be 1M+
  2. No nulls in critical columns
  3. Weekly_Sales must be non-negative
  4. Fuel_Price must be realistic
  5. Store IDs must be 1-45 only
  6. Holiday types must be one of 5 expected values
  7. No duplicate Store+Dept+Date rows
"""

import pandas as pd
from loguru import logger


class DataQualityError(Exception):
    """Raised when a data quality check fails. Stops the pipeline."""
    pass


# ─── Check 1: Row Count ───────────────────────────────────────────────────────
def check_row_count(df: pd.DataFrame, minimum: int) -> None:
    """
    Assert the DataFrame has at least minimum rows.
    Guards against silent data loss in the transform phase.
    """
    if len(df) < minimum:
        raise DataQualityError(
            f'Row count {len(df):,} is below minimum {minimum:,}. '
            f'Something went wrong in the transform phase.'
        )
    logger.info(f'[VALIDATE] Row count: {len(df):,} >= {minimum:,} — PASS')


# ─── Check 2: No Nulls ───────────────────────────────────────────────────────
def check_no_nulls(df: pd.DataFrame, cols: list) -> None:
    """
    Assert no null values exist in the specified critical columns.
    These columns are required for the star schema foreign keys.
    """
    null_counts = df[cols].isnull().sum()
    failed_cols = null_counts[null_counts > 0]
    if not failed_cols.empty:
        raise DataQualityError(
            f'Null values found in critical columns: {failed_cols.to_dict()}'
        )
    logger.info(f'[VALIDATE] No nulls in {cols} — PASS')


# ─── Check 3: Value Range ─────────────────────────────────────────────────────
def check_value_range(
    df: pd.DataFrame,
    col: str,
    low: float,
    high: float
) -> None:
    """
    Assert all values in col fall within [low, high].
    Catches outliers and data corruption early.
    """
    out_of_range = df[(df[col] < low) | (df[col] > high)]
    if len(out_of_range) > 0:
        raise DataQualityError(
            f'{col}: {len(out_of_range):,} values outside range [{low}, {high}]. '
            f'Min={df[col].min():.2f}, Max={df[col].max():.2f}'
        )
    logger.info(f'[VALIDATE] {col} within [{low}, {high}] — PASS')


# ─── Check 4: Uniqueness ──────────────────────────────────────────────────────
def check_uniqueness(df: pd.DataFrame, cols: list) -> None:
    """
    Assert no duplicate rows exist on the given composite key.
    Store+Dept+Date must be unique — duplicates corrupt aggregations.
    """
    n_dupes = df.duplicated(subset=cols).sum()
    if n_dupes > 0:
        raise DataQualityError(
            f'Found {n_dupes:,} duplicate rows on key {cols}'
        )
    logger.info(f'[VALIDATE] Uniqueness on {cols} — PASS')


# ─── Check 5: Referential Integrity ──────────────────────────────────────────
def check_referential_integrity(
    df: pd.DataFrame,
    col: str,
    valid_values: set
) -> None:
    """
    Assert all values in col exist in the valid_values set.
    Prevents orphaned foreign keys when loading to PostgreSQL.
    """
    unknowns = set(df[col].unique()) - valid_values
    if unknowns:
        raise DataQualityError(
            f'{col}: unexpected values found: {unknowns}'
        )
    logger.info(f'[VALIDATE] Referential integrity on {col} — PASS')


# ─── Master Validate Function ─────────────────────────────────────────────────
def validate_final(df: pd.DataFrame) -> None:
    """
    Run all 7 data quality checks on the final transformed DataFrame.
    Called from run_pipeline.py BEFORE loading to PostgreSQL.

    If any check fails, DataQualityError is raised and the pipeline
    stops immediately — nothing bad gets written to the database.
    """
    logger.info('=' * 60)
    logger.info('[VALIDATE] DATA QUALITY CHECKS START')
    logger.info('=' * 60)

    # Check 1: Must have 1M+ rows
    check_row_count(df, 1_000_000)

    # Check 2: Critical columns must not have nulls
    check_no_nulls(df, ['Store', 'Dept', 'Date', 'Weekly_Sales', 'IsHoliday'])

    # Check 3: Sales must be non-negative and realistic
    check_value_range(df, 'Weekly_Sales', 0, 10_000_000)

    # Check 4: Fuel price must be realistic (between $0.50 and $20)
    check_value_range(df, 'Fuel_Price', 0.5, 20.0)

    # Check 5: Store IDs must be 1-45 only
    check_referential_integrity(df, 'Store', set(range(1, 46)))

    # Check 6: Holiday types must be one of the 5 expected values
    check_referential_integrity(
        df,
        'holiday_type',
        {'None', 'Super_Bowl', 'Labor_Day', 'Thanksgiving', 'Christmas'}
    )

    # Check 7: No duplicate Store+Dept+Date rows (natural key uniqueness)
    check_uniqueness(df, ['Store', 'Dept', 'Date'])

    logger.success('[VALIDATE] ALL 7 CHECKS PASSED — data is clean and ready to load')