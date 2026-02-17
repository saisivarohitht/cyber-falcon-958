# Stripe MRR Dashboard Project

## Project Overview

This project creates realistic Stripe test data with historical billing patterns for MRR analytics and dashboard development. It uses Stripe's Test Clock functionality to simulate time progression and generate authentic billing cycles with auto-pay.


## Step-by-Step Installation

### 1. Clone the Repository

```bash
git clone git@github.com:saisivarohitht/cyber-falcon-958.git
cd cyber-falcon-958
```

### 2. Stripe Setup

1. Go to https://dashboard.stripe.com/register
2. Complete the signup process
3. Navigate to **Developers** → **API keys**
4. Ensure you're in **Test mode** (toggle in the top right)
5. Copy your **Secret key** (starts with `sk_test_`)
6. Save this for the `.env` file

### 3. Google Cloud Platform Setup

#### Create a GCP Project

1. Go to https://console.cloud.google.com
2. Click **Select a project** → **New Project**
3. Name your project (e.g., "mrr-dashboard")
4. Click **Create**

#### Enable BigQuery API

1. In the GCP Console, go to **APIs & Services** → **Library**
2. Search for "BigQuery API"
3. Click **Enable**

#### Create Service Account

1. Go to **IAM & Admin** → **Service Accounts**
2. Click **Create Service Account**
3. Name: "bigquery-mrr-service"
4. Click **Create and Continue**
5. Role: Select **BigQuery Admin**
6. Click **Done**

#### Download Credentials

1. Find your service account in the list
2. Click the three dots → **Manage keys**
3. Click **Add Key** → **Create new key**
4. Select **JSON**
5. Click **Create**
6. The JSON file will download automatically
7. Save this file in the project directory (e.g., `gcp-credentials.json`)

### 4. Environment Configuration

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Edit `.env` with your actual values:

```env
STRIPE_TEST_SECRET_KEY=sk_test_your_secret_key_here
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/gcp-credentials.json
GOOGLE_CLOUD_PROJECT_ID=your-project-id
BQ_DATASET_ID=stripe_data
```

**Important**: Use the absolute path for `GOOGLE_APPLICATION_CREDENTIALS`


### 5. Create a Virtual Environment and Install Dependencies
   
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

### 6. Install Frontend Dependencies

```bash
cd frontend
npm install
cd ..
```

### 7. Generate MRR Test Data
   ```bash
   python scripts/generate_test_data.py
   ```

### 8. Export Stripe Data to BigQuery
   ```bash
   python scripts/stripe_to_bigquery.py
   ```

### 9. Run Backend
   ```bash
   python backend/api_server.py
   ```

### 10. Run Frontend
   Open a new terminal window and navigate to the project directory. Execute the following statements: 
   ```bash
   source venv/bin/activate
   cd frontend
   npm start
   ```
   On the browser, open http://localhost:3000/

## Files

### Scripts
- `scripts/generate_test_data.py` - Main script to generate 100 test customers with 6 months of billing history using Stripe Test Clocks with parallel test clock advancement
- `scripts/stripe_to_bigquery.py` - ETL pipeline to extract Stripe data and load into BigQuery with MRR calculations

### Backend
- `backend/api_server.py` - Flask REST API server for MRR dashboard data

### Frontend
- `frontend/index.html` - Main HTML entry point
- `frontend/src/App.jsx` - React dashboard application with MRR charts and metrics
- `frontend/src/main.jsx` - React application bootstrap
- `frontend/package.json` - Node.js dependencies (React, Recharts, Tailwind CSS)
- `frontend/vite.config.js` - Vite build configuration with API proxy

### SQL Queries
- `sql/mrr_monthly_metrics.sql` - Monthly MRR calculation with growth metrics (used by pipeline)
- `sql/cohort_analysis.sql` - Customer retention cohort analysis
- `sql/mrr_queries.sql` - Sample dashboard queries for BigQuery

### Configuration
- `requirements.txt` - Python dependencies (Stripe, BigQuery, Flask, pandas)
- `.env.example` - Environment variables template (Stripe keys, GCP credentials)
- `gcp-credentials.json` - Google Cloud service account key (Downloaded from BigQuery)


## Usage Notes

- Test Clocks have a 3 customer limit in Stripe test mode
- Customers and subscriptions are tied to the Test Clock
- Invoices are automatically generated and paid when advancing the clock
- Data is suitable for BigQuery extraction and MRR dashboard development
