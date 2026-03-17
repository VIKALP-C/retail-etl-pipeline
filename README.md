# 🛒 Retail Sales ETL Pipeline & Analytics Dashboard

## Overview
End-to-end ETL pipeline ingesting **1,681,140 records** from **5 data sources**
into a **PostgreSQL star schema** with **Power BI analytics dashboard**.

## Pipeline Architecture
```
5 CSV Sources → Python ETL → PostgreSQL Star Schema → Power BI Dashboard
```

## Tech Stack
| Layer | Technology |
|-------|------------|
| Language | Python 3.11 |
| ETL | Pandas, NumPy, SQLAlchemy, psycopg2 |
| Database | PostgreSQL 16 (Star Schema) |
| Data Quality | Custom validation layer (7 checks) |
| Visualization | Microsoft Power BI Desktop |
| Version Control | Git & GitHub |

## Data Sources
| Source | Description | Rows |
|--------|-------------|------|
| Walmart train.csv | Weekly sales per store+dept | 421,570 |
| Walmart features.csv | Store economic features | 8,190 |
| Walmart stores.csv | Store metadata | 45 |
| FRED CPIAUCSL | US monthly CPI | 949 |
| FRED UNRATE | US monthly unemployment | 937 |

## Pipeline Results
- ✅ 1,681,140 rows processed
- ✅ 7 data quality checks passed
- ✅ 4 dimension tables + 1 fact table loaded
- ✅ Pipeline completes in 3 minutes

## Dashboard Screenshots

### Page 1 — Executive Summary
![Executive Summary](screenshots/page1_executive_summary.png)

### Page 2 — Holiday Demand Analysis
![Holiday Deep Dive](screenshots/page2_holiday_deep_dive.png)

### Page 3 — CPI & Inflation Impact
![CPI Inflation](screenshots/page3_cpi_inflation.png)

### Page 4 — Fuel Price Impact
![Fuel Price](screenshots/page4_fuel_price.png)

### Page 5 — Store Performance
![Store Performance](screenshots/page5_store_performance.png)

## Project Structure
```
retail_etl_pipeline/
├── data/raw/          # Raw CSV files (5 sources)
├── etl/
│   ├── extract.py     # Extract layer
│   ├── transform.py   # Transform layer
│   ├── load.py        # Load layer
│   └── validate.py    # Data quality checks
├── sql/
│   ├── create_schema.sql  # Star schema DDL
│   └── queries.sql        # Analytics queries
├── screenshots/       # Dashboard screenshots
├── dashboard/         # Power BI .pbix file
├── logs/              # Pipeline logs
└── run_pipeline.py    # Master orchestrator
```

## How to Run
1. Clone this repo:
```bash
git clone https://github.com/YOURUSERNAME/retail-etl-pipeline.git
```
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Download datasets and place in data/raw/
4. Configure .env with your PostgreSQL credentials
5. Run the pipeline:
```bash
python run_pipeline.py
```

## Key Findings
- 📈 **7% average sales lift** during holiday weeks
- 🛢️ **Fuel prices rising** from $2.80 to $4.30 over 2010-2017
- 📊 **Type A stores** generate 3x more revenue than Type C
- 💹 **CPI rising** from 218 to 257 showing consistent inflation trend
- 🏪 **Store 20** is the top performing store overall