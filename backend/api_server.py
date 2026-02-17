#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MRR Analytics API
=================
Flask API to serve BigQuery MRR data to the React dashboard.
"""

from flask import Flask, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT_ID', 'your-project-id')
DATASET_ID = os.getenv('BQ_DATASET_ID', 'stripe_mrr_analytics')

# BigQuery setup - Use service account credentials
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# If credentials path is relative, resolve it from the project root
if credentials_path and not os.path.isabs(credentials_path):
    credentials_path = str(Path(__file__).parent.parent / credentials_path)

# Initialize BigQuery client with service account
bq_client = None

def get_bigquery_client():
    """Get authenticated BigQuery client using service account."""
    global bq_client
    
    if bq_client is not None:
        return bq_client
    
    try:
        if credentials_path and os.path.exists(credentials_path):
            # Use service account credentials from file
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
            print(f"✅ BigQuery client initialized with service account: {credentials_path}")
        else:
            # Fallback to Application Default Credentials
            bq_client = bigquery.Client(project=PROJECT_ID)
            print(f"✅ BigQuery client initialized with Application Default Credentials")
        
        return bq_client
    except Exception as e:
        print(f"❌ BigQuery authentication failed: {e}")
        raise e

@app.route('/api/mrr-trend')
def get_mrr_trend():
    """Get MRR trend data."""
    client = get_bigquery_client()
    query = """
    SELECT 
        month_year,
        FORMAT_DATE('%b %Y', month_start_date) as month_label,
        total_mrr,
        new_mrr,
        churned_mrr,
        net_new_mrr,
        active_customers,
        churned_customers,
        growth_rate,
        churn_rate,
        average_revenue_per_user as arpu
    FROM `{project}.{dataset}.mrr_monthly_summary`
    ORDER BY month_start_date
    """.format(project=PROJECT_ID, dataset=DATASET_ID)
    
    results = client.query(query).result()
    data = []
    for row in results:
        data.append({
            'month': row.month_year,
            'monthLabel': row.month_label,
            'totalMrr': float(row.total_mrr or 0),
            'newMrr': float(row.new_mrr or 0),
            'churnedMrr': float(row.churned_mrr or 0),
            'netNewMrr': float(row.net_new_mrr or 0),
            'activeCustomers': int(row.active_customers or 0),
            'churnedCustomers': int(row.churned_customers or 0),
            'growthRate': float(row.growth_rate or 0),
            'churnRate': float(row.churn_rate or 0) * 100,  # Convert to percentage
            'arpu': float(row.arpu or 0)
        })
    return jsonify(data)

@app.route('/api/subscriptions')
def get_subscriptions():
    """Get subscription breakdown by status."""
    client = get_bigquery_client()
    query = """
    SELECT 
        status,
        COUNT(*) as count,
        SUM(mrr_amount) as mrr
    FROM `{project}.{dataset}.subscriptions`
    GROUP BY status
    ORDER BY count DESC
    """.format(project=PROJECT_ID, dataset=DATASET_ID)
    
    results = client.query(query).result()
    data = []
    for row in results:
        data.append({
            'status': row.status,
            'count': row.count,
            'mrr': float(row.mrr or 0)
        })
    return jsonify(data)

@app.route('/api/summary')
def get_summary():
    """Get summary metrics."""
    client = get_bigquery_client()
    
    # Get latest MRR data
    mrr_query = """
    SELECT * FROM `{project}.{dataset}.mrr_monthly_summary`
    ORDER BY month_start_date DESC LIMIT 1
    """.format(project=PROJECT_ID, dataset=DATASET_ID)
    mrr_result = list(client.query(mrr_query).result())[0]
    
    # Get subscription counts
    sub_query = """
    SELECT 
        COUNTIF(status = 'active') as active,
        COUNTIF(status = 'canceled') as canceled,
        COUNTIF(status = 'past_due') as past_due
    FROM `{project}.{dataset}.subscriptions`
    """.format(project=PROJECT_ID, dataset=DATASET_ID)
    sub_result = list(client.query(sub_query).result())[0]
    
    return jsonify({
        'currentMrr': float(mrr_result.total_mrr or 0),
        'activeCustomers': int(mrr_result.active_customers or 0),
        'arpu': float(mrr_result.average_revenue_per_user or 0),
        'churnRate': float(mrr_result.churn_rate or 0) * 100,
        'growthRate': float(mrr_result.growth_rate or 0),
        'subscriptions': {
            'active': sub_result.active,
            'canceled': sub_result.canceled,
            'pastDue': sub_result.past_due
        }
    })

@app.route('/api/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    print("Starting MRR Analytics API on http://localhost:5001")
    app.run(debug=True, port=5001)
