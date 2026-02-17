# Stripe MRR Dashboard Project

A complete Stripe MRR (Monthly Recurring Revenue) test dataset generator for dashboard analytics using Stripe Test Clocks.

## Project Overview

This project creates realistic Stripe test data with historical billing patterns for MRR analytics and dashboard development. It uses Stripe's Test Clock functionality to simulate time progression and generate authentic billing cycles with auto-pay.

## Features

- **Test Clock Integration** - Uses Stripe Test Clocks for historical billing simulation  
- **Auto-Pay Simulation** - Customers with payment methods and automatic charge collection  
- **Multiple Plan Tiers** - Starter ($29.0), Professional ($99.0), Business ($149.0), Enterprise ($299.0)  
- **Historical Data** - 6+ months of billing history with paid invoices  
- **BigQuery Ready** - Data structure suitable for analytics and dashboards  
- **Clean Environment** - Utilities for easy cleanup and regeneration  

## Setup

1. **Create a Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   - Copy `.env.example` to `.env`
   - Add your Stripe test API keys
   - Steps to add GOOGLE_APPLICATION_CREDENTIALS 
   - Add your GOOGLE_CLOUD_PROJECT_ID and BQ_DATASET_ID

3. **Generate MRR Test Data**
   ```bash
   python scripts/generate_test_data.py
   ```

4. **Export Stripe Data to BigQuery**
   ```bash
   python scripts/stripe_to_bigquery.py
   ```

5. **Steps to Run SQL**

6. **Run Backend**
   ```bash
   python backend/api_server.py

7. **Run Frontend**
   - Open a new terminal window and navigate to the project directory.
   - Run 
   ```bash
   source venv/bin/activate
   cd frontend
   npm start
   ```

## Files

- `stripe_mrr_generator.py` - Main script to generate MRR test data using Test Clocks
- `cleanup.py` - Utility to clean up all test data from Stripe
- `requirements.txt` - Python dependencies
- `setup.sh` - Environment setup script
- `.env.example` - Environment variables template

## Data Generated

- **3 Test Customers** (Test Clock limit)
- **3 Active Subscriptions** across different plan tiers
- **20+ Historical Invoices** with proper amounts and paid status
- **6+ Months** of billing history through Test Clock advancement
- **Auto-Pay Enabled** with payment methods attached

## Usage Notes

- Test Clocks have a 3 customer limit in Stripe test mode
- Customers and subscriptions are tied to the Test Clock
- Invoices are automatically generated and paid when advancing the clock
- Data is suitable for BigQuery extraction and MRR dashboard development

## Next Steps

This test data can be used for:
- Building MRR analytics dashboards
- Testing BigQuery data pipelines
- Developing revenue reporting features
- Stripe webhook handling development

## Cleanup

To clean up all test data:
```bash
python cleanup.py
```
