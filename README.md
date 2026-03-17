# Retail Sales ETL Pipeline & Analytics Dashboard

## Overview
End-to-end ETL pipeline ingesting 1.68M+ records from 5 data sources
into a PostgreSQL star schema with Power BI analytics dashboard.

## Tech Stack
- Python 3.11
- PostgreSQL 16
- Pandas, NumPy, SQLAlchemy, psycopg2
- Power BI Desktop
- Git & GitHub

## Data Sources
- Walmart Store Sales (Kaggle)
- Walmart Store Features (Kaggle)
- Walmart Store Metadata (Kaggle)
- US CPI Data (FRED)
- US Unemployment Rate (FRED)

## Pipeline Results
- 1,681,140 rows processed
- 7 data quality checks passed
- 5 dimension + fact tables loaded
- Pipeline completes in 3 minutes

## Project Structure
retail_etl_pipeline/
├── data/raw/          # Raw CSV files
├── etl/
│   ├── extract.py     # Extract layer
│   ├── transform.py   # Transform layer
│   ├── load.py        # Load layer
│   └── validate.py    # Data quality
├── sql/               # PostgreSQL schema
├── dashboard/         # Power BI file
├── logs/              # Pipeline logs
└── run_pipeline.py    # Master orchestrator

## How to Run
1. Clone this repo
2. pip install -r requirements.txt
3. Configure .env with PostgreSQL credentials
4. python run_pipeline.py

## Dashboard Pages
1. Executive Summary
2. Holiday Demand Analysis
3. CPI & Inflation Impact
4. Fuel Price Impact
5. Store Performance Analysis
