"""
run_pipeline.py

PURPOSE: Master ETL pipeline orchestrator.
Run this file to execute the complete ETL pipeline:
  Extract → Transform → Validate → Load

USAGE:
  python run_pipeline.py

LOG OUTPUT: Logs go to console AND to logs/pipeline_YYYY-MM-DD.log
"""

import sys
import os
from datetime import datetime
from loguru import logger

# Import our ETL modules
from etl.extract   import extract_all
from etl.transform import transform_all
from etl.validate  import validate_final
from etl.load      import load_all


def setup_logging() -> None:
    """Configure loguru: write to console + timestamped log file."""
    os.makedirs('logs', exist_ok=True)
    log_path = f"logs/pipeline_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

    # Remove default handler
    logger.remove()

    # Console: colored, human-readable
    logger.add(
        sys.stdout,
        colorize=True,
        format='<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}',
        level='INFO'
    )

    # File: detailed log with timestamps
    logger.add(
        log_path,
        rotation='50 MB',
        format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}',
        level='DEBUG'
    )
    logger.info(f'Log file: {log_path}')


def main() -> None:
    setup_logging()
    start_time = datetime.now()

    logger.info('█' * 60)
    logger.info('  RETAIL SALES ETL PIPELINE — STARTING')
    logger.info('  5 Data Sources → PostgreSQL Star Schema')
    logger.info('█' * 60)

    try:
        # ── PHASE 1: EXTRACT ──────────────────────────────────────────
        logger.info('PHASE 1: EXTRACT')
        raw_data = extract_all()

        # ── PHASE 2: TRANSFORM ────────────────────────────────────────
        logger.info('PHASE 2: TRANSFORM')
        final_df = transform_all(raw_data)

        # ── PHASE 3: VALIDATE ─────────────────────────────────────────
        logger.info('PHASE 3: VALIDATE')
        validate_final(final_df)

        # ── PHASE 4: LOAD ─────────────────────────────────────────────
        logger.info('PHASE 4: LOAD')
        load_all(final_df)

        # ── DONE ──────────────────────────────────────────────────────
        elapsed = datetime.now() - start_time
        logger.success('█' * 60)
        logger.success('  PIPELINE COMPLETE')
        logger.success(f'  Total rows loaded : {len(final_df):,}')
        logger.success(f'  Total time        : {elapsed}')
        logger.success('█' * 60)

    except Exception as e:
        elapsed = datetime.now() - start_time
        logger.error('█' * 60)
        logger.error(f'  PIPELINE FAILED after {elapsed}')
        logger.error(f'  Error: {e}')
        logger.error('█' * 60)
        sys.exit(1)  # Exit with error code so CI/CD systems detect failure


if __name__ == '__main__':
    main()