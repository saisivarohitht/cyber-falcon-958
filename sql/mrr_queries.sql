-- MRR Dashboard Queries
-- =====================
-- Sample queries for MRR analysis and dashboard visualization.
-- These queries reference the BigQuery tables created by stripe_to_bigquery.py


-- ============================================================================
-- QUERY: mrr_by_plan
-- Current MRR breakdown by pricing plan
-- ============================================================================
SELECT 
  p.name as plan_name,
  pr.nickname as price_nickname,
  COUNT(DISTINCT s.customer_id) as customers,
  SUM(s.mrr_amount) as total_mrr,
  AVG(s.mrr_amount) as avg_mrr_per_customer
FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions` s
JOIN `{PROJECT_ID}.{DATASET_ID}.prices` pr ON s.price_id = pr.price_id
JOIN `{PROJECT_ID}.{DATASET_ID}.products` p ON s.product_id = p.product_id
WHERE s.status = 'active'
GROUP BY p.name, pr.nickname, pr.unit_amount
ORDER BY total_mrr DESC;


-- ============================================================================
-- QUERY: mrr_growth_trend
-- MRR Growth Trend over time
-- ============================================================================
SELECT 
  month_year,
  total_mrr,
  new_mrr,
  churned_mrr,
  net_new_mrr,
  growth_rate,
  active_customers,
  churn_rate
FROM `{PROJECT_ID}.{DATASET_ID}.mrr_monthly_summary`
ORDER BY month_start_date;


-- ============================================================================
-- QUERY: customer_churn_analysis
-- Customer Churn Analysis by month
-- ============================================================================
SELECT 
  DATE_TRUNC(DATE(canceled_at), MONTH) as churn_month,
  COUNT(*) as churned_customers,
  SUM(mrr_amount) as churned_mrr,
  AVG(DATE_DIFF(DATE(canceled_at), DATE(start_date), DAY)) as avg_lifetime_days
FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions`
WHERE status = 'canceled' AND canceled_at IS NOT NULL
GROUP BY churn_month
ORDER BY churn_month;


-- ============================================================================
-- QUERY: revenue_by_collection_method
-- Revenue by Collection Method (auto-pay vs invoice)
-- ============================================================================
SELECT 
  s.collection_method,
  COUNT(DISTINCT s.customer_id) as customers,
  SUM(s.mrr_amount) as total_mrr,
  COUNT(DISTINCT i.invoice_id) as total_invoices,
  COUNT(DISTINCT CASE WHEN i.status = 'paid' THEN i.invoice_id END) as paid_invoices,
  SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN i.status = 'paid' THEN i.invoice_id END),
    COUNT(DISTINCT i.invoice_id)
  ) * 100 as payment_success_rate
FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions` s
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.invoices` i ON s.subscription_id = i.subscription_id
WHERE s.status IN ('active', 'past_due')
GROUP BY s.collection_method;


-- ============================================================================
-- QUERY: subscription_status_summary
-- Subscription Status Summary
-- ============================================================================
SELECT 
  status,
  COUNT(*) as count,
  SUM(mrr_amount) as mrr
FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions`
GROUP BY status
ORDER BY count DESC;


-- ============================================================================
-- QUERY: mrr_trend_analysis
-- MRR Trend Analysis with all metrics
-- ============================================================================
SELECT 
  month_year,
  month_start_date,
  total_mrr,
  new_mrr,
  churned_mrr,
  net_new_mrr,
  active_customers,
  churned_customers,
  growth_rate,
  churn_rate,
  average_revenue_per_user
FROM `{PROJECT_ID}.{DATASET_ID}.mrr_monthly_summary`
ORDER BY month_start_date;

