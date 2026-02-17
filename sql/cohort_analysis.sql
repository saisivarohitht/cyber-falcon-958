-- Customer Cohort Retention Analysis
-- ===================================
-- Calculates customer retention rates by acquisition cohort.
-- Groups customers by their first subscription month and tracks their retention over time.

WITH 
-- Get cohort month for each customer (first subscription start)
customer_cohorts AS (
  SELECT 
    customer_id,
    DATE_TRUNC(MIN(DATE(start_date)), MONTH) as cohort_start_date,
    FORMAT_DATE('%Y-%m', MIN(DATE(start_date))) as cohort_month
  FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions`
  GROUP BY customer_id
),

-- Generate month series
date_range AS (
  SELECT MIN(cohort_start_date) as min_date, CURRENT_DATE() as max_date
  FROM customer_cohorts
),

months AS (
  SELECT month_start
  FROM date_range,
  UNNEST(GENERATE_DATE_ARRAY(
    DATE_TRUNC(min_date, MONTH),
    DATE_TRUNC(max_date, MONTH),
    INTERVAL 1 MONTH
  )) as month_start
),

-- For each cohort and period, calculate retention
cohort_periods AS (
  SELECT 
    cc.cohort_month,
    cc.cohort_start_date,
    m.month_start,
    DATE_DIFF(m.month_start, cc.cohort_start_date, MONTH) as period_number,
    cc.customer_id
  FROM customer_cohorts cc
  CROSS JOIN months m
  WHERE m.month_start >= cc.cohort_start_date
),

-- Check if customer was active in each period
cohort_activity AS (
  SELECT 
    cp.cohort_month,
    cp.cohort_start_date,
    cp.period_number,
    cp.customer_id,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions` s
        WHERE s.customer_id = cp.customer_id
          AND DATE(s.start_date) <= DATE_ADD(cp.month_start, INTERVAL 1 MONTH)
          AND (s.canceled_at IS NULL OR DATE(s.canceled_at) >= cp.month_start)
      ) THEN 1 ELSE 0
    END as is_active
  FROM cohort_periods cp
),

-- Aggregate by cohort and period
cohort_summary AS (
  SELECT 
    cohort_month,
    cohort_start_date,
    period_number,
    COUNT(DISTINCT customer_id) as customers_in_cohort,
    SUM(is_active) as active_customers
  FROM cohort_activity
  GROUP BY cohort_month, cohort_start_date, period_number
)

SELECT 
  cohort_month,
  cohort_start_date,
  period_number,
  (SELECT COUNT(DISTINCT customer_id) FROM customer_cohorts WHERE cohort_month = cs.cohort_month) as customers_in_cohort,
  active_customers,
  SAFE_DIVIDE(active_customers, (SELECT COUNT(DISTINCT customer_id) FROM customer_cohorts WHERE cohort_month = cs.cohort_month)) as retention_rate,
  0.0 as cohort_revenue,
  0.0 as revenue_per_customer,
  CURRENT_TIMESTAMP() as calculated_at
FROM cohort_summary cs
WHERE period_number <= 12  -- Limit to 12 months of retention
ORDER BY cohort_start_date, period_number
