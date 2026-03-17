-- sql/queries.sql
-- Business analytics queries for the retail sales data warehouse.
-- Run these in pgAdmin or VS Code SQLTools after the pipeline completes.

-- ════════════════════════════════════
-- QUERY 1: Total sales by year
-- ════════════════════════════════════
SELECT
    dt.year,
    SUM(f.weekly_sales)        AS total_sales,
    AVG(f.fuel_price)          AS avg_fuel_price,
    AVG(f.bls_cpi)             AS avg_cpi,
    AVG(f.national_unemployment) AS avg_unemployment
FROM fact_sales f
JOIN dim_date dt ON f.date_key = dt.date_key
GROUP BY dt.year
ORDER BY dt.year;


-- ════════════════════════════════════
-- QUERY 2: Holiday vs non-holiday sales comparison
-- ════════════════════════════════════
SELECT
    h.holiday_type,
    COUNT(*)                          AS total_weeks,
    ROUND(AVG(f.weekly_sales), 2)     AS avg_weekly_sales,
    ROUND(SUM(f.weekly_sales), 2)     AS total_sales
FROM fact_sales f
JOIN dim_holiday h ON f.holiday_key = h.holiday_key
GROUP BY h.holiday_type
ORDER BY avg_weekly_sales DESC;


-- ════════════════════════════════════
-- QUERY 3: Top 10 departments by holiday sales lift
-- ════════════════════════════════════
SELECT
    d.dept_id,
    ROUND(AVG(CASE WHEN f.is_holiday THEN f.weekly_sales END), 2)     AS holiday_avg,
    ROUND(AVG(CASE WHEN NOT f.is_holiday THEN f.weekly_sales END), 2) AS normal_avg,
    ROUND(
        (AVG(CASE WHEN f.is_holiday THEN f.weekly_sales END) /
         NULLIF(AVG(CASE WHEN NOT f.is_holiday THEN f.weekly_sales END), 0) - 1) * 100
    , 2) AS holiday_lift_pct
FROM fact_sales f
JOIN dim_dept d ON f.dept_key = d.dept_key
GROUP BY d.dept_id
ORDER BY holiday_lift_pct DESC
LIMIT 10;


-- ════════════════════════════════════
-- QUERY 4: Monthly sales trend with CPI overlay
-- ════════════════════════════════════
SELECT
    dt.year,
    dt.month,
    SUM(f.weekly_sales)                                        AS total_sales,
    AVG(f.cpi)                                                 AS avg_internal_cpi,
    AVG(f.bls_cpi)                                             AS avg_bls_cpi,
    AVG(f.fuel_price)                                          AS avg_fuel,
    ROUND(SUM(f.weekly_sales) / NULLIF(AVG(f.bls_cpi), 0), 2) AS cpi_normalized_sales
FROM fact_sales f
JOIN dim_date dt ON f.date_key = dt.date_key
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;


-- ════════════════════════════════════
-- QUERY 5: Store performance ranking by type
-- ════════════════════════════════════
SELECT
    s.store_id,
    s.store_type,
    s.store_size,
    s.size_bucket,
    ROUND(SUM(f.weekly_sales), 2) AS total_revenue,
    RANK() OVER (
        PARTITION BY s.store_type
        ORDER BY SUM(f.weekly_sales) DESC
    ) AS rank_within_type
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.store_id, s.store_type, s.store_size, s.size_bucket
ORDER BY total_revenue DESC;


-- ════════════════════════════════════
-- QUERY 6: Fuel price impact on sales
-- ════════════════════════════════════
SELECT
    ROUND(f.fuel_price, 1)            AS fuel_price_bucket,
    COUNT(*)                          AS num_weeks,
    ROUND(AVG(f.weekly_sales), 2)     AS avg_weekly_sales,
    ROUND(SUM(f.weekly_sales), 2)     AS total_sales
FROM fact_sales f
WHERE f.fuel_price IS NOT NULL
GROUP BY ROUND(f.fuel_price, 1)
ORDER BY fuel_price_bucket;


-- ════════════════════════════════════
-- QUERY 7: Row count check per table
-- ════════════════════════════════════
SELECT 'fact_sales'  AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL
SELECT 'dim_store',  COUNT(*) FROM dim_store
UNION ALL
SELECT 'dim_date',   COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_dept',   COUNT(*) FROM dim_dept
UNION ALL
SELECT 'dim_holiday',COUNT(*) FROM dim_holiday
ORDER BY row_count DESC;
```

Save both files:
- `sql/create_schema.sql`
- `sql/queries.sql`

Now you have every single file needed. Your complete file checklist:
```
✅ etl/__init__.py       (empty)
✅ etl/extract.py        (Phase 7)
✅ etl/transform.py      (Phase 8)
✅ etl/validate.py       (Phase 10)
✅ etl/load.py           (Phase 9)
✅ sql/create_schema.sql (Phase 9)
✅ sql/queries.sql       (analytics)
✅ run_pipeline.py       (Phase 11)
✅ .env                  (credentials)
✅ requirements.txt      (packages)