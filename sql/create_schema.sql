-- sql/create_schema.sql
-- Creates the star schema data warehouse for retail sales analytics.
-- Run this once to set up the database structure.

-- ════════════════════════════════════
-- DIMENSION TABLES
-- ════════════════════════════════════

-- dim_store: one row per store
CREATE TABLE IF NOT EXISTS dim_store (
    store_key   SERIAL       PRIMARY KEY,
    store_id    SMALLINT     NOT NULL UNIQUE,
    store_type  CHAR(1)      NOT NULL CHECK (store_type IN ('A','B','C')),
    store_size  INTEGER      NOT NULL CHECK (store_size > 0),
    size_bucket VARCHAR(10)  NOT NULL
);

-- dim_date: one row per unique date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      SERIAL    PRIMARY KEY,
    full_date     DATE      NOT NULL UNIQUE,
    year          SMALLINT  NOT NULL,
    quarter       SMALLINT  NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month         SMALLINT  NOT NULL CHECK (month  BETWEEN 1 AND 12),
    week_of_year  SMALLINT  NOT NULL,
    is_month_end  BOOLEAN   NOT NULL DEFAULT FALSE
);

-- dim_dept: one row per department
CREATE TABLE IF NOT EXISTS dim_dept (
    dept_key  SERIAL    PRIMARY KEY,
    dept_id   SMALLINT  NOT NULL UNIQUE
);

-- dim_holiday: one row per holiday type (including 'None')
CREATE TABLE IF NOT EXISTS dim_holiday (
    holiday_key   SERIAL      PRIMARY KEY,
    holiday_type  VARCHAR(20) NOT NULL UNIQUE
);

-- ════════════════════════════════════
-- FACT TABLE
-- ════════════════════════════════════

-- fact_sales: one row per store+dept+date combination
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id                BIGSERIAL     PRIMARY KEY,
    store_key              INT           NOT NULL REFERENCES dim_store(store_key),
    date_key               INT           NOT NULL REFERENCES dim_date(date_key),
    dept_key               INT           NOT NULL REFERENCES dim_dept(dept_key),
    holiday_key            INT           NOT NULL REFERENCES dim_holiday(holiday_key),

    -- Raw sales measures
    weekly_sales           NUMERIC(12,2) NOT NULL CHECK (weekly_sales >= 0),
    temperature            NUMERIC(6,2),
    fuel_price             NUMERIC(6,3),

    -- Walmart internal CPI and unemployment
    cpi                    NUMERIC(9,4),
    unemployment           NUMERIC(6,3),

    -- External economic indicators (FRED sources)
    bls_cpi                NUMERIC(9,4),   -- From CPIAUCSL (Source 4)
    national_unemployment  NUMERIC(6,3),   -- From UNRATE   (Source 5)

    -- Engineered features
    total_markdown         NUMERIC(12,2),
    sales_4wk_avg          NUMERIC(12,2),
    sales_vs_dept_avg      NUMERIC(8,4),
    cpi_normalized_sales   NUMERIC(12,2),
    is_holiday             BOOLEAN       NOT NULL
);

-- ════════════════════════════════════
-- PERFORMANCE INDEXES
-- ════════════════════════════════════

-- These speed up Power BI queries significantly
CREATE INDEX IF NOT EXISTS idx_fact_store   ON fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_date    ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_dept    ON fact_sales(dept_key);
CREATE INDEX IF NOT EXISTS idx_fact_holiday ON fact_sales(holiday_key);
CREATE INDEX IF NOT EXISTS idx_date_year    ON dim_date(year);
CREATE INDEX IF NOT EXISTS idx_date_month   ON dim_date(year, month);