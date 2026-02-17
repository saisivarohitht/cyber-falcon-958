#!/usr/bin/env python3
"""
Stripe to BigQuery MRR Data Pipeline
=====================================
Extracts MRR data from Stripe and loads it into Google BigQuery for analytics.
Creates optimized tables for MRR dashboards and reporting.
"""

import stripe
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import json
import pandas as pd
from typing import Dict, List, Any
import time

# Load environment variables from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Set up clients
stripe_client = stripe.StripeClient(api_key=os.getenv('STRIPE_TEST_SECRET_KEY'))

# Configuration
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT_ID', 'your-project-id')
DATASET_ID = os.getenv('BQ_DATASET_ID', 'stripe_mrr_v2')
LOCATION = os.getenv('BQ_LOCATION', 'US')

# BigQuery setup - Use GOOGLE_APPLICATION_CREDENTIALS from .env
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# If credentials path is relative, resolve it from the project root
if credentials_path and not os.path.isabs(credentials_path):
    credentials_path = str(Path(__file__).parent.parent / credentials_path)

try:
    if credentials_path and os.path.exists(credentials_path):
        # Use service account credentials from file
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        print(f"✅ Using BigQuery client with service account: {credentials_path}")
    else:
        # Fallback to Application Default Credentials (gcloud auth)
        bq_client = bigquery.Client(project=PROJECT_ID)
        print(f"✅ Using BigQuery client with Application Default Credentials")
except Exception as e:
    print(f"❌ BigQuery authentication failed: {e}")
    print(f"💡 Options:")
    print(f"   1. Set GOOGLE_APPLICATION_CREDENTIALS in .env to your service account key file")
    print(f"   2. Run: gcloud auth application-default login")
    raise e

# SQL file directory
SQL_DIR = Path(__file__).parent.parent / 'sql'


def load_sql_file(filename: str, **kwargs) -> str:
    """
    Load a SQL file from the sql/ directory and substitute placeholders.
    
    Args:
        filename: Name of the SQL file (e.g., 'mrr_monthly_metrics.sql')
        **kwargs: Additional variables to substitute (beyond PROJECT_ID and DATASET_ID)
    
    Returns:
        SQL query string with placeholders substituted
    
    Example:
        query = load_sql_file('mrr_monthly_metrics.sql')
    """
    sql_path = SQL_DIR / filename
    
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    with open(sql_path, 'r') as f:
        sql_content = f.read()
    
    # Substitute standard placeholders
    sql_content = sql_content.replace('{PROJECT_ID}', PROJECT_ID)
    sql_content = sql_content.replace('{DATASET_ID}', DATASET_ID)
    
    # Substitute any additional placeholders
    for key, value in kwargs.items():
        sql_content = sql_content.replace(f'{{{key}}}', str(value))
    
    return sql_content


def load_sql_queries_from_file(filename: str) -> Dict[str, str]:
    """
    Load multiple named SQL queries from a single file.
    
    Queries are delimited by comments like:
    -- QUERY: query_name
    
    Args:
        filename: Name of the SQL file containing multiple queries
    
    Returns:
        Dictionary mapping query names to their SQL content
    """
    sql_content = load_sql_file(filename)
    queries = {}
    current_query_name = None
    current_query_lines = []
    
    for line in sql_content.split('\n'):
        # Check for query delimiter
        if line.strip().upper().startswith('-- QUERY:'):
            # Save previous query if exists
            if current_query_name:
                queries[current_query_name] = '\n'.join(current_query_lines).strip()
            # Start new query
            current_query_name = line.split(':', 1)[1].strip().lower()
            current_query_lines = []
        elif current_query_name:
            # Skip comment lines that describe the query
            if not (line.strip().startswith('-- ') and line.strip().endswith('=')):
                current_query_lines.append(line)
    
    # Save last query
    if current_query_name:
        queries[current_query_name] = '\n'.join(current_query_lines).strip()
    
    return queries


class StripeToBigQueryPipeline:
    """
    Pipeline to extract Stripe MRR data and load into BigQuery.
    """
    
    def __init__(self):
        self.dataset_ref = bq_client.dataset(DATASET_ID, project=PROJECT_ID)
        self.sql_dir = SQL_DIR
        self.tables = {
            'customers': 'customers',
            'subscriptions': 'subscriptions', 
            'invoices': 'invoices',
            'prices': 'prices',
            'products': 'products',
            'mrr_summary': 'mrr_monthly_summary',
            'cohort_analysis': 'customer_cohorts'
        }
        
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist."""
        try:
            bq_client.get_dataset(self.dataset_ref)
            print(f"✅ Dataset {DATASET_ID} already exists")
        except NotFound:
            print(f"📦 Creating BigQuery dataset: {DATASET_ID}")
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = LOCATION
            dataset.description = "Stripe MRR analytics data for dashboard and reporting"
            bq_client.create_dataset(dataset)
            print(f"✅ Created dataset: {DATASET_ID}")
    
    def create_table_schemas(self) -> Dict[str, List[bigquery.SchemaField]]:
        """Define BigQuery table schemas optimized for MRR analytics."""
        
        schemas = {
            'customers': [
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("delinquent", "BOOLEAN"),
                bigquery.SchemaField("test_clock_id", "STRING"),
                bigquery.SchemaField("default_payment_method", "STRING"),
                bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            
            'subscriptions': [
                bigquery.SchemaField("subscription_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("current_period_start", "TIMESTAMP"),
                bigquery.SchemaField("current_period_end", "TIMESTAMP"),
                bigquery.SchemaField("start_date", "TIMESTAMP"),
                bigquery.SchemaField("ended_at", "TIMESTAMP"),
                bigquery.SchemaField("canceled_at", "TIMESTAMP"),
                bigquery.SchemaField("cancel_at_period_end", "BOOLEAN"),
                bigquery.SchemaField("collection_method", "STRING"),
                bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("price_id", "STRING"),
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("unit_amount", "INTEGER"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("mrr_amount", "FLOAT", mode="REQUIRED"),  # Monthly recurring revenue
                bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            
            'invoices': [
                bigquery.SchemaField("invoice_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("invoice_number", "STRING"),  # Human-readable invoice number
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("subscription_id", "STRING"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("amount_due", "INTEGER"),
                bigquery.SchemaField("amount_paid", "INTEGER"),
                bigquery.SchemaField("amount_remaining", "INTEGER"),
                bigquery.SchemaField("subtotal", "INTEGER"),
                bigquery.SchemaField("total", "INTEGER"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("due_date", "TIMESTAMP"),
                bigquery.SchemaField("period_start", "TIMESTAMP"),
                bigquery.SchemaField("period_end", "TIMESTAMP"),
                bigquery.SchemaField("paid_at", "TIMESTAMP"),
                bigquery.SchemaField("collection_method", "STRING"),
                bigquery.SchemaField("hosted_invoice_url", "STRING"),
                bigquery.SchemaField("invoice_pdf", "STRING"),
                bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            
            'prices': [
                bigquery.SchemaField("price_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("active", "BOOLEAN"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("unit_amount", "INTEGER"),
                bigquery.SchemaField("recurring_interval", "STRING"),
                bigquery.SchemaField("recurring_interval_count", "INTEGER"),
                bigquery.SchemaField("nickname", "STRING"),
                bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            
            'products': [
                bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("active", "BOOLEAN"),
                bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("updated", "TIMESTAMP"),
                bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            
            'mrr_summary': [
                bigquery.SchemaField("month_year", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("month_start_date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("total_mrr", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("new_mrr", "FLOAT"),
                bigquery.SchemaField("expansion_mrr", "FLOAT"),
                bigquery.SchemaField("contraction_mrr", "FLOAT"),
                bigquery.SchemaField("churned_mrr", "FLOAT"),
                bigquery.SchemaField("net_new_mrr", "FLOAT"),
                bigquery.SchemaField("active_customers", "INTEGER"),
                bigquery.SchemaField("new_customers", "INTEGER"),
                bigquery.SchemaField("churned_customers", "INTEGER"),
                bigquery.SchemaField("average_revenue_per_user", "FLOAT"),
                bigquery.SchemaField("churn_rate", "FLOAT"),
                bigquery.SchemaField("growth_rate", "FLOAT"),
                bigquery.SchemaField("calculated_at", "TIMESTAMP", mode="REQUIRED"),
            ],
            
            'cohort_analysis': [
                bigquery.SchemaField("cohort_month", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("cohort_start_date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("period_number", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("customers_in_cohort", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("active_customers", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("retention_rate", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("cohort_revenue", "FLOAT"),
                bigquery.SchemaField("revenue_per_customer", "FLOAT"),
                bigquery.SchemaField("calculated_at", "TIMESTAMP", mode="REQUIRED"),
            ]
        }
        
        return schemas
    
    def create_tables(self):
        """Create BigQuery tables with optimized schemas."""
        schemas = self.create_table_schemas()
        
        for table_key, table_name in self.tables.items():
            table_ref = self.dataset_ref.table(table_name)
            
            try:
                bq_client.get_table(table_ref)
                print(f"✅ Table {table_name} already exists")
            except NotFound:
                print(f"📋 Creating table: {table_name}")
                table = bigquery.Table(table_ref, schema=schemas[table_key])
                
                # Add partitioning for time-series tables
                if table_key in ['invoices', 'mrr_summary', 'cohort_analysis']:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field="extracted_at" if table_key == 'invoices' else "calculated_at"
                    )
                
                # Add clustering for better query performance
                if table_key == 'subscriptions':
                    table.clustering_fields = ["status", "customer_id"]
                elif table_key == 'invoices':
                    table.clustering_fields = ["status", "customer_id"]
                
                bq_client.create_table(table)
                print(f"✅ Created table: {table_name}")
    
    def extract_stripe_data(self) -> Dict[str, List[Dict]]:
        """Extract all relevant data from Stripe."""
        print("\n🔄 Extracting data from Stripe...")
        
        extracted_data = {
            'customers': [],
            'subscriptions': [],
            'invoices': [],
            'prices': [],
            'products': []
        }
        
        extraction_time = datetime.utcnow()
        
        # First, get all test clocks to find customers associated with them
        print("  🕐 Checking for test clocks...")
        test_clock_ids = []
        import stripe as stripe_module
        stripe_module.api_key = os.getenv('STRIPE_TEST_SECRET_KEY')
        
        try:
            test_clocks = stripe_module.test_helpers.TestClock.list(limit=100)
            test_clock_ids = [tc.id for tc in test_clocks.data]
            print(f"     Found {len(test_clock_ids)} test clocks")
        except Exception as e:
            print(f"     No test clocks found or error: {e}")
        
        # Extract customers - including those on test clocks
        print("  👥 Extracting customers...")
        
        # Method 1: Get customers from test clocks FIRST (most reliable for test data)
        if test_clock_ids:
            print("     Getting customers from test clocks...")
            for tc_id in test_clock_ids:
                try:
                    # Use the test_clock filter parameter to get customers on this test clock
                    customers = stripe_module.Customer.list(limit=100, test_clock=tc_id)
                    print(f"       Test clock {tc_id}: {len(customers.data)} customers")
                    for customer in customers.data:
                        # Get test clock as string ID
                        tc_value = customer.test_clock
                        if hasattr(tc_value, 'id'):
                            tc_value = tc_value.id
                        
                        extracted_data['customers'].append({
                            'customer_id': customer.id,
                            'email': customer.email,
                            'name': customer.name,
                            'description': customer.description,
                            'created': datetime.fromtimestamp(customer.created),
                            'currency': customer.currency,
                            'delinquent': customer.delinquent,
                            'test_clock_id': tc_value,
                            'default_payment_method': customer.invoice_settings.default_payment_method if customer.invoice_settings else None,
                            'extracted_at': extraction_time
                        })
                except Exception as e:
                    print(f"       Error getting customers for test clock {tc_id}: {e}")
        
        # Method 2: Try regular customers (without test clocks)
        if len(extracted_data['customers']) == 0:
            print("     Trying regular customer list...")
            try:
                customers = stripe_module.Customer.list(limit=100)
                for customer in customers.data:
                    extracted_data['customers'].append({
                        'customer_id': customer.id,
                        'email': customer.email,
                        'name': customer.name,
                        'description': customer.description,
                        'created': datetime.fromtimestamp(customer.created),
                        'currency': customer.currency,
                        'delinquent': customer.delinquent,
                        'test_clock_id': getattr(customer, 'test_clock', None),
                        'default_payment_method': customer.invoice_settings.default_payment_method if customer.invoice_settings else None,
                        'extracted_at': extraction_time
                    })
            except Exception as e:
                print(f"     Error listing customers: {e}")
        
        # Method 3: Extract customer info from invoices if still empty
        if len(extracted_data['customers']) == 0:
            print("     Extracting customer info from invoices...")
            customer_ids_seen = set()
            try:
                invoices = stripe_module.Invoice.list(limit=100)
                for invoice in invoices.data:
                    if invoice.customer and invoice.customer not in customer_ids_seen:
                        customer_ids_seen.add(invoice.customer)
                        # Fetch the customer directly by ID
                        try:
                            customer = stripe_module.Customer.retrieve(invoice.customer)
                            # Get test clock as string ID
                            tc_value = getattr(customer, 'test_clock', None)
                            if hasattr(tc_value, 'id'):
                                tc_value = tc_value.id
                            
                            extracted_data['customers'].append({
                                'customer_id': customer.id,
                                'email': customer.email,
                                'name': customer.name,
                                'description': customer.description,
                                'created': datetime.fromtimestamp(customer.created),
                                'currency': customer.currency,
                                'delinquent': customer.delinquent,
                                'test_clock_id': tc_value,
                                'default_payment_method': customer.invoice_settings.default_payment_method if customer.invoice_settings else None,
                                'extracted_at': extraction_time
                            })
                        except Exception as e:
                            print(f"     Could not fetch customer {invoice.customer}: {e}")
            except Exception as e:
                print(f"     Error extracting from invoices: {e}")
        
        print(f"     Found {len(extracted_data['customers'])} customers")
        
        # Extract products
        print("  📦 Extracting products...")
        products = stripe_module.Product.list(limit=100)
        for product in products.data:
            extracted_data['products'].append({
                'product_id': product.id,
                'name': product.name,
                'description': product.description,
                'active': product.active,
                'created': datetime.fromtimestamp(product.created),
                'updated': datetime.fromtimestamp(product.updated),
                'extracted_at': extraction_time
            })
        
        # Extract prices
        print("  💰 Extracting prices...")
        prices = stripe_module.Price.list(limit=100)
        for price in prices.data:
            recurring = price.recurring
            extracted_data['prices'].append({
                'price_id': price.id,
                'product_id': price.product,
                'active': price.active,
                'currency': price.currency,
                'unit_amount': price.unit_amount,
                'recurring_interval': recurring.interval if recurring else None,
                'recurring_interval_count': recurring.interval_count if recurring else None,
                'nickname': price.nickname,
                'created': datetime.fromtimestamp(price.created),
                'extracted_at': extraction_time
            })
        
        # Extract subscriptions - try multiple methods
        print("  📋 Extracting subscriptions...")
        
        # Method 1: Get subscriptions from customers (most reliable for test clock data)
        if len(extracted_data['customers']) > 0:
            print("     Getting subscriptions from customers...")
            for cust_data in extracted_data['customers']:
                try:
                    subs = stripe_module.Subscription.list(
                        customer=cust_data['customer_id'],
                        status='all',
                        limit=100
                    )
                    for subscription in subs.data:
                        self._add_subscription_to_data(subscription, extracted_data, extraction_time)
                except Exception as e:
                    print(f"       Error for customer {cust_data['customer_id']}: {e}")
        
        # Method 2: Try regular subscription list if none found
        if len(extracted_data['subscriptions']) == 0:
            print("     Trying regular subscription list...")
            try:
                subscriptions = stripe_module.Subscription.list(limit=100, status='all')
                for subscription in subscriptions.data:
                    self._add_subscription_to_data(subscription, extracted_data, extraction_time)
            except Exception as e:
                print(f"     Error listing subscriptions: {e}")
        
        # Method 3: Extract subscription info from invoices as fallback
        if len(extracted_data['subscriptions']) == 0:
            print("     Extracting subscription info from invoices...")
            subscription_ids_seen = set()
            try:
                invoices = stripe_module.Invoice.list(limit=100)
                for invoice in invoices.data:
                    sub_id = getattr(invoice, 'subscription', None)
                    if sub_id and sub_id not in subscription_ids_seen:
                        subscription_ids_seen.add(sub_id)
                        try:
                            subscription = stripe_module.Subscription.retrieve(sub_id)
                            self._add_subscription_to_data(subscription, extracted_data, extraction_time)
                        except Exception as e:
                            print(f"     Could not fetch subscription {sub_id}: {e}")
            except Exception as e:
                print(f"     Error extracting from invoices: {e}")
        
        print(f"     Found {len(extracted_data['subscriptions'])} subscriptions")
        
        # Extract invoices - get invoices for each customer (needed for test clock customers)
        print("  🧾 Extracting invoices...")
        invoice_ids_seen = set()
        
        # Method 1: Get invoices per customer (works for test clock customers)
        for cust_data in extracted_data['customers']:
            try:
                invoices = stripe_module.Invoice.list(customer=cust_data['customer_id'], limit=100)
                for invoice in invoices.data:
                    if invoice.id not in invoice_ids_seen:
                        invoice_ids_seen.add(invoice.id)
                        extracted_data['invoices'].append({
                            'invoice_id': invoice.id,
                            'invoice_number': invoice.number,
                            'customer_id': invoice.customer,
                            'subscription_id': getattr(invoice, 'subscription', None),
                            'status': invoice.status,
                            'amount_due': invoice.amount_due,
                            'amount_paid': invoice.amount_paid,
                            'amount_remaining': invoice.amount_remaining,
                            'subtotal': invoice.subtotal,
                            'total': invoice.total,
                            'currency': invoice.currency,
                            'created': datetime.fromtimestamp(invoice.created),
                            'due_date': datetime.fromtimestamp(invoice.due_date) if invoice.due_date else None,
                            'period_start': datetime.fromtimestamp(invoice.period_start) if invoice.period_start else None,
                            'period_end': datetime.fromtimestamp(invoice.period_end) if invoice.period_end else None,
                            'paid_at': datetime.fromtimestamp(invoice.status_transitions.paid_at) if invoice.status_transitions and invoice.status_transitions.paid_at else None,
                            'collection_method': invoice.collection_method,
                            'hosted_invoice_url': invoice.hosted_invoice_url,
                            'invoice_pdf': invoice.invoice_pdf,
                            'extracted_at': extraction_time
                        })
            except Exception as e:
                print(f"       Error getting invoices for customer {cust_data['customer_id']}: {e}")
        
        # Method 2: Fallback to regular invoice list if none found
        if len(extracted_data['invoices']) == 0:
            print("     Trying regular invoice list...")
            try:
                invoices = stripe_module.Invoice.list(limit=100)
                for invoice in invoices.data:
                    if invoice.id not in invoice_ids_seen:
                        invoice_ids_seen.add(invoice.id)
                        extracted_data['invoices'].append({
                            'invoice_id': invoice.id,
                            'invoice_number': invoice.number,
                            'customer_id': invoice.customer,
                            'subscription_id': getattr(invoice, 'subscription', None),
                            'status': invoice.status,
                            'amount_due': invoice.amount_due,
                            'amount_paid': invoice.amount_paid,
                            'amount_remaining': invoice.amount_remaining,
                            'subtotal': invoice.subtotal,
                            'total': invoice.total,
                            'currency': invoice.currency,
                            'created': datetime.fromtimestamp(invoice.created),
                            'due_date': datetime.fromtimestamp(invoice.due_date) if invoice.due_date else None,
                            'period_start': datetime.fromtimestamp(invoice.period_start) if invoice.period_start else None,
                            'period_end': datetime.fromtimestamp(invoice.period_end) if invoice.period_end else None,
                            'paid_at': datetime.fromtimestamp(invoice.status_transitions.paid_at) if invoice.status_transitions and invoice.status_transitions.paid_at else None,
                            'collection_method': invoice.collection_method,
                            'hosted_invoice_url': invoice.hosted_invoice_url,
                            'invoice_pdf': invoice.invoice_pdf,
                            'extracted_at': extraction_time
                        })
            except Exception as e:
                print(f"     Error listing invoices: {e}")
        
        print(f"✅ Extracted data summary:")
        for data_type, data_list in extracted_data.items():
            print(f"  • {data_type}: {len(data_list)} records")
        
        return extracted_data
    
    def _add_subscription_to_data(self, subscription, extracted_data, extraction_time):
        """Helper to add a subscription to the extracted data."""
        # Check if already added
        existing_ids = [s['subscription_id'] for s in extracted_data['subscriptions']]
        if subscription.id in existing_ids:
            return
        
        try:
            # Calculate MRR amount
            mrr_amount = 0
            price_id = None
            product_id = None
            unit_amount = None
            quantity = 1
            current_period_start = None
            current_period_end = None
            
            # Access items using dictionary notation (subscription.items is a method, not an attribute)
            items_data = []
            try:
                items_data = subscription["items"]["data"]
            except (KeyError, TypeError):
                pass
            
            if items_data:
                item = items_data[0]
                # Access item fields using dictionary notation
                price_data = item["price"]
                
                unit_amount = price_data.get("unit_amount", 0) or 0
                quantity = item.get("quantity", 1) or 1
                price_id = price_data.get("id")
                product_id = price_data.get("product")
                
                # Get current period from item (not subscription level)
                current_period_start = item.get("current_period_start")
                current_period_end = item.get("current_period_end")
                
                # Convert to monthly amount based on interval
                recurring = price_data.get("recurring", {})
                if recurring:
                    interval = recurring.get("interval", "month")
                    if interval == 'month':
                        mrr_amount = (unit_amount * quantity) / 100
                    elif interval == 'year':
                        mrr_amount = (unit_amount * quantity) / 12 / 100
                    elif interval == 'week':
                        mrr_amount = (unit_amount * quantity) * 4.33 / 100
            
            # Use billing_cycle_anchor or start_date as fallback for period dates
            if not current_period_start:
                current_period_start = subscription.get("billing_cycle_anchor") or subscription.get("start_date") or subscription.created
            if not current_period_end:
                current_period_end = current_period_start  # Fallback
            
            # Build subscription record using dict access for all fields
            extracted_data['subscriptions'].append({
                'subscription_id': subscription.id,
                'customer_id': subscription["customer"],
                'status': subscription["status"],
                'current_period_start': datetime.fromtimestamp(current_period_start) if current_period_start else extraction_time,
                'current_period_end': datetime.fromtimestamp(current_period_end) if current_period_end else extraction_time,
                'start_date': datetime.fromtimestamp(subscription["start_date"]) if subscription.get("start_date") else extraction_time,
                'ended_at': datetime.fromtimestamp(subscription["ended_at"]) if subscription.get("ended_at") else None,
                'canceled_at': datetime.fromtimestamp(subscription["canceled_at"]) if subscription.get("canceled_at") else None,
                'cancel_at_period_end': subscription.get("cancel_at_period_end", False),
                'collection_method': subscription.get("collection_method"),
                'created': datetime.fromtimestamp(subscription["created"]) if subscription.get("created") else extraction_time,
                'currency': subscription.get("currency", "usd"),
                'price_id': price_id,
                'product_id': product_id,
                'unit_amount': unit_amount,
                'quantity': quantity,
                'mrr_amount': mrr_amount,
                'extracted_at': extraction_time
            })
        except Exception as e:
            print(f"     Error processing subscription {subscription.id}: {e}")
    
    def load_data_to_bigquery(self, data: Dict[str, List[Dict]]):
        """Load extracted data into BigQuery tables."""
        print("\n📤 Loading data to BigQuery...")
        
        for data_type, records in data.items():
            if not records:
                print(f"  ⚠️  No data to load for {data_type}")
                continue
                
            table_name = self.tables[data_type]
            table_ref = self.dataset_ref.table(table_name)
            
            print(f"  📋 Loading {len(records)} records to {table_name}...")
            
            # Configure load job - use JSON instead of Parquet
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace existing data
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.autodetect = False  # Use explicit schema
            
            # Set schema based on table type
            schemas = self.create_table_schemas()
            job_config.schema = schemas[data_type]
            
            # Convert records to JSON lines
            import io
            json_data = io.StringIO()
            for record in records:
                # Convert datetime objects to strings for JSON serialization
                json_record = {}
                for key, value in record.items():
                    if isinstance(value, datetime):
                        json_record[key] = value.isoformat()
                    else:
                        json_record[key] = value
                json_data.write(json.dumps(json_record, default=str) + '\n')
            
            json_data.seek(0)
            
            # Load data
            try:
                job = bq_client.load_table_from_file(json_data, table_ref, job_config=job_config)
                job.result()  # Wait for job to complete
                
                print(f"  ✅ Loaded {len(records)} records to {table_name}")
                
            except Exception as e:
                print(f"  ❌ Failed to load {table_name}: {e}")
                # Print first record for debugging
                if records:
                    print(f"  📝 Sample record: {json.dumps(records[0], indent=2, default=str)}")
    
    def calculate_mrr_metrics(self):
        """Calculate MRR summary metrics and store in BigQuery."""
        print("\n📊 Calculating MRR metrics...")
        print(f"   Loading query from: sql/mrr_monthly_metrics.sql")
        
        # Load SQL query from external file
        mrr_query = load_sql_file('mrr_monthly_metrics.sql')
        
        # Execute query and load results
        try:
            query_job = bq_client.query(mrr_query)
            results = query_job.result()
            
            # Convert to list of dicts
            mrr_records = []
            for row in results:
                # Calculate growth rate
                growth_rate = 0.0
                if row.prev_month_mrr and row.prev_month_mrr > 0:
                    growth_rate = ((row.total_mrr - row.prev_month_mrr) / row.prev_month_mrr) * 100
                
                mrr_records.append({
                    'month_year': row.month_year,
                    'month_start_date': row.month_start_date,
                    'total_mrr': float(row.total_mrr or 0),
                    'new_mrr': float(row.new_mrr or 0),
                    'expansion_mrr': float(row.expansion_mrr or 0),
                    'contraction_mrr': float(row.contraction_mrr or 0),
                    'churned_mrr': float(row.churned_mrr or 0),
                    'net_new_mrr': float(row.net_new_mrr or 0),
                    'active_customers': int(row.active_customers or 0),
                    'new_customers': int(row.new_customers or 0),
                    'churned_customers': int(row.churned_customers or 0),
                    'average_revenue_per_user': float(row.average_revenue_per_user or 0),
                    'churn_rate': float(row.churn_rate or 0),
                    'growth_rate': growth_rate,
                    'calculated_at': row.calculated_at
                })
            
            # Load MRR summary data
            if mrr_records:
                self.load_data_to_bigquery({'mrr_summary': mrr_records})
                print(f"✅ Calculated MRR metrics for {len(mrr_records)} months")
            else:
                print("⚠️  No MRR data to calculate")
                
        except Exception as e:
            print(f"❌ Error calculating MRR metrics: {e}")
    
    def calculate_cohort_analysis(self):
        """Calculate customer cohort retention analysis and store in BigQuery."""
        print("\n📊 Calculating cohort analysis...")
        print(f"   Loading query from: sql/cohort_analysis.sql")
        
        # Load SQL query from external file
        cohort_query = load_sql_file('cohort_analysis.sql')
        
        try:
            query_job = bq_client.query(cohort_query)
            results = query_job.result()
            
            cohort_records = []
            for row in results:
                cohort_records.append({
                    'cohort_month': row.cohort_month,
                    'cohort_start_date': row.cohort_start_date,
                    'period_number': int(row.period_number),
                    'customers_in_cohort': int(row.customers_in_cohort),
                    'active_customers': int(row.active_customers),
                    'retention_rate': float(row.retention_rate or 0),
                    'cohort_revenue': float(row.cohort_revenue or 0),
                    'revenue_per_customer': float(row.revenue_per_customer or 0),
                    'calculated_at': row.calculated_at
                })
            
            if cohort_records:
                self.load_data_to_bigquery({'cohort_analysis': cohort_records})
                print(f"✅ Calculated cohort analysis for {len(cohort_records)} cohort-periods")
            else:
                print("⚠️  No cohort data to calculate")
                
        except Exception as e:
            print(f"❌ Error calculating cohort analysis: {e}")
    
    def generate_sample_queries(self):
        """Generate sample SQL queries for MRR analysis."""
        print("\n📝 Sample BigQuery queries for MRR analysis:")
        print(f"   Queries loaded from: sql/mrr_queries.sql")
        
        # Load queries from external SQL file
        try:
            queries = load_sql_queries_from_file('mrr_queries.sql')
            
            for query_name, query in queries.items():
                print(f"\n-- {query_name.replace('_', ' ').title()}")
                # Print first 10 lines of each query to keep output manageable
                query_lines = query.strip().split('\n')
                preview_lines = query_lines[:10]
                print('\n'.join(preview_lines))
                if len(query_lines) > 10:
                    print(f"   ... ({len(query_lines) - 10} more lines)")
                    
        except FileNotFoundError:
            print("   ⚠️  sql/mrr_queries.sql not found, using inline queries")
            self._generate_inline_sample_queries()
        except Exception as e:
            print(f"   ⚠️  Error loading queries: {e}")
            self._generate_inline_sample_queries()
    
    def _generate_inline_sample_queries(self):
        """Fallback: generate inline sample queries if SQL files are unavailable."""
        queries = {
            "Current MRR by Plan": f"""
            SELECT 
              p.name as plan_name,
              pr.nickname as price_nickname,
              COUNT(DISTINCT s.customer_id) as customers,
              SUM(s.mrr_amount) as total_mrr
            FROM `{PROJECT_ID}.{DATASET_ID}.subscriptions` s
            JOIN `{PROJECT_ID}.{DATASET_ID}.prices` pr ON s.price_id = pr.price_id
            JOIN `{PROJECT_ID}.{DATASET_ID}.products` p ON s.product_id = p.product_id
            WHERE s.status = 'active'
            GROUP BY p.name, pr.nickname
            ORDER BY total_mrr DESC
            """,
            "MRR Growth Trend": f"""
            SELECT month_year, total_mrr, new_mrr, churned_mrr, growth_rate
            FROM `{PROJECT_ID}.{DATASET_ID}.mrr_monthly_summary`
            ORDER BY month_start_date
            """
        }
        
        for query_name, query in queries.items():
            print(f"\n-- {query_name}")
            print(query)
    
    def run_full_pipeline(self):
        """Execute the complete Stripe to BigQuery pipeline."""
        print("🚀 Starting Stripe to BigQuery MRR Pipeline")
        print("=" * 50)
        
        try:
            # Step 1: Setup
            self.create_dataset_if_not_exists()
            self.create_tables()
            
            # Step 2: Extract data from Stripe
            extracted_data = self.extract_stripe_data()
            
            # Step 3: Load data to BigQuery
            self.load_data_to_bigquery(extracted_data)
            
            # Step 4: Calculate MRR metrics
            self.calculate_mrr_metrics()
            
            # Step 5: Calculate cohort analysis
            self.calculate_cohort_analysis()
            
            # Step 6: Generate sample queries
            self.generate_sample_queries()
            
            print("\n✅ Pipeline completed successfully!")
            print("🎯 Your Stripe MRR data is now ready for dashboard creation in BigQuery!")
            
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    # Create and run the pipeline
    pipeline = StripeToBigQueryPipeline()
    pipeline.run_full_pipeline()
