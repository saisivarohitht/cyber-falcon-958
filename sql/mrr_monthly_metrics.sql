-- MRR Monthly Metrics Calculation
-- ================================
-- Calculates Monthly Recurring Revenue (MRR) summary metrics from subscription data.
-- This query creates a month series and checks which subscriptions were active during each month.
-- Key logic: A subscription contributes to MRR if it started before month end AND 
-- (hasn't been canceled OR was canceled after month start)

WITH 
-- Generate a series of months from the earliest subscription to now
date_range AS (
  SELECT MIN(DATE(start_date)) as min_date, CURRENT_DATE() as max_date
  FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions`
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

-- For each month, determine which subscriptions were active
monthly_subscriptions AS (
  SELECT 
    m.month_start as month_start_date,
    FORMAT_DATE('%Y-%m', m.month_start) as month_year,
    s.customer_id,
    s.subscription_id,
    s.status,
    s.mrr_amount,
    s.start_date,
    s.canceled_at,
    s.ended_at,
    -- A subscription is active in a month if:
    -- 1. It started on or before the end of the month
    -- 2. It hasn't been canceled OR was canceled after the END of the month
    -- (if canceled during the month, it shouldn't count for that month's MRR)
    CASE 
      WHEN DATE(s.start_date) <= DATE_ADD(m.month_start, INTERVAL 1 MONTH)
        AND (s.canceled_at IS NULL OR DATE(s.canceled_at) >= DATE_ADD(m.month_start, INTERVAL 1 MONTH))
      THEN TRUE
      ELSE FALSE
    END as was_active_in_month
  FROM months m
  CROSS JOIN `{PROJECT_ID}.{DATASET_ID}.subscriptions` s
),

monthly_metrics AS (
  SELECT 
    month_year,
    month_start_date,
    SUM(CASE WHEN was_active_in_month THEN mrr_amount ELSE 0 END) as total_mrr,
    COUNT(DISTINCT CASE WHEN was_active_in_month THEN customer_id END) as active_customers,
    COUNT(DISTINCT CASE WHEN DATE(start_date) >= month_start_date 
                           AND DATE(start_date) < DATE_ADD(month_start_date, INTERVAL 1 MONTH)
                           THEN customer_id END) as new_customers,
    COUNT(DISTINCT CASE WHEN canceled_at IS NOT NULL
                           AND DATE(canceled_at) >= month_start_date 
                           AND DATE(canceled_at) < DATE_ADD(month_start_date, INTERVAL 1 MONTH) 
                           THEN customer_id END) as churned_customers,
    SUM(CASE WHEN DATE(start_date) >= month_start_date 
                  AND DATE(start_date) < DATE_ADD(month_start_date, INTERVAL 1 MONTH)
                  THEN mrr_amount ELSE 0 END) as new_mrr,
    SUM(CASE WHEN canceled_at IS NOT NULL
                  AND DATE(canceled_at) >= month_start_date 
                  AND DATE(canceled_at) < DATE_ADD(month_start_date, INTERVAL 1 MONTH) 
                  THEN mrr_amount ELSE 0 END) as churned_mrr
  FROM monthly_subscriptions
  GROUP BY month_year, month_start_date
)

SELECT 
  month_year,
  month_start_date,
  total_mrr,
  new_mrr,
  0.0 as expansion_mrr,
  0.0 as contraction_mrr,
  churned_mrr,
  (new_mrr - churned_mrr) as net_new_mrr,
  active_customers,
  new_customers,
  churned_customers,
  SAFE_DIVIDE(total_mrr, active_customers) as average_revenue_per_user,
  SAFE_DIVIDE(churned_customers, active_customers) as churn_rate,
  LAG(total_mrr) OVER (ORDER BY month_start_date) as prev_month_mrr,
  CURRENT_TIMESTAMP() as calculated_at
FROM monthly_metrics
WHERE active_customers > 0 OR new_customers > 0 OR churned_customers > 0
ORDER BY month_start_date
