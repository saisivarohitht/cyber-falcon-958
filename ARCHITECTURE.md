# Architecture & Flow Diagrams

This document contains visual diagrams of the Stripe MRR Analytics pipeline.

## 1. High-Level Architecture

```mermaid
flowchart LR
    subgraph Source["Data Source"]
        S[("🟣 Stripe API")]
    end
    
    subgraph ETL["ETL Pipeline"]
        P["🐍 Python Scripts"]
    end
    
    subgraph Warehouse["Data Warehouse"]
        B[("📊 BigQuery")]
    end
    
    subgraph Backend["API Layer"]
        F["🌐 Flask API"]
    end
    
    subgraph Frontend["Presentation"]
        R["⚛️ React Dashboard"]
    end
    
    S -->|"Extract"| P
    P -->|"Load"| B
    B -->|"SQL Queries"| F
    F -->|"JSON API"| R
```

## 2. Data Pipeline Flow

```mermaid
flowchart TB
    subgraph Stripe["Stripe Test Environment"]
        TC["Test Clocks"]
        CU["Customers"]
        SU["Subscriptions"]
        IN["Invoices"]
        PR["Products & Prices"]
    end
    
    subgraph ETL["scripts/stripe_to_bigquery.py"]
        EX["Extract Data"]
        TR["Transform & Calculate MRR"]
        LD["Load to BigQuery"]
    end
    
    subgraph BigQuery["BigQuery Tables"]
        T1["customers"]
        T2["subscriptions"]
        T3["invoices"]
        T4["prices"]
        T5["products"]
        T6["mrr_monthly_summary"]
    end
    
    TC --> CU
    CU --> SU
    SU --> IN
    
    CU --> EX
    SU --> EX
    IN --> EX
    PR --> EX
    
    EX --> TR
    TR --> LD
    
    LD --> T1
    LD --> T2
    LD --> T3
    LD --> T4
    LD --> T5
    TR --> T6
```

## 3. API Request Flow

```mermaid
sequenceDiagram
    participant User
    participant React as React Dashboard<br/>(localhost:3000)
    participant Vite as Vite Proxy
    participant Flask as Flask API<br/>(localhost:5001)
    participant BQ as BigQuery

    User->>React: Open Dashboard
    React->>Vite: GET /api/mrr-trend
    Vite->>Flask: Proxy Request
    Flask->>BQ: SQL Query
    BQ-->>Flask: Query Results
    Flask-->>Vite: JSON Response
    Vite-->>React: JSON Data
    React->>React: Render Charts
    React-->>User: Display Dashboard
```

## 4. MRR Calculation Logic

```mermaid
flowchart TD
    subgraph Input["Raw Subscription Data"]
        A["Subscription Records"]
        B["Start Date"]
        C["Cancel Date"]
        D["Price Amount"]
        E["Billing Interval"]
    end
    
    subgraph Calculate["MRR Calculation"]
        F{"Interval Type?"}
        G["Monthly: amount"]
        H["Yearly: amount / 12"]
        I["Weekly: amount × 4.33"]
        J["MRR per Subscription"]
    end
    
    subgraph Aggregate["Monthly Aggregation"]
        K["Group by Month"]
        L["Sum Active MRR"]
        M["Count New Customers"]
        N["Count Churned"]
        O["Calculate Growth Rate"]
    end
    
    subgraph Output["mrr_monthly_summary"]
        P["total_mrr"]
        Q["new_mrr"]
        R["churned_mrr"]
        S["active_customers"]
        T["churn_rate"]
    end
    
    A --> F
    D --> F
    E --> F
    F -->|monthly| G
    F -->|yearly| H
    F -->|weekly| I
    G --> J
    H --> J
    I --> J
    
    J --> K
    B --> K
    C --> K
    
    K --> L
    K --> M
    K --> N
    L --> O
    
    L --> P
    M --> Q
    N --> R
    K --> S
    N --> T
```

## 5. Component Architecture

```mermaid
flowchart TB
    subgraph Frontend["frontend/"]
        subgraph Components["React Components"]
            App["App.jsx"]
            MC["MetricCard"]
            Charts["Recharts"]
        end
        
        subgraph Config["Configuration"]
            Vite["vite.config.js"]
            TW["tailwind.config.js"]
        end
    end
    
    subgraph Backend["scripts/"]
        API["api_server.py"]
        ETL["stripe_to_bigquery.py"]
        GEN["generate_test_data.py"]
    end
    
    subgraph Data["sql/"]
        MRR["mrr_calculation.sql"]
        QRY["mrr_queries.sql"]
    end
    
    App --> MC
    App --> Charts
    Vite -->|proxy /api| API
    API -->|uses| MRR
    API -->|uses| QRY
    ETL -->|creates| MRR
```

## 6. Data Model (ERD)

```mermaid
erDiagram
    CUSTOMERS {
        string customer_id PK
        string email
        string name
        timestamp created
        boolean delinquent
    }
    
    SUBSCRIPTIONS {
        string subscription_id PK
        string customer_id FK
        string status
        string price_id FK
        string product_id FK
        float mrr_amount
        timestamp start_date
        timestamp canceled_at
    }
    
    INVOICES {
        string invoice_id PK
        string customer_id FK
        string subscription_id FK
        string status
        integer total
        timestamp created
        timestamp paid_at
    }
    
    PRODUCTS {
        string product_id PK
        string name
        boolean active
    }
    
    PRICES {
        string price_id PK
        string product_id FK
        integer unit_amount
        string recurring_interval
    }
    
    MRR_MONTHLY_SUMMARY {
        string month_year PK
        date month_start_date
        float total_mrr
        float new_mrr
        float churned_mrr
        integer active_customers
        float churn_rate
        float growth_rate
    }
    
    CUSTOMERS ||--o{ SUBSCRIPTIONS : has
    CUSTOMERS ||--o{ INVOICES : receives
    SUBSCRIPTIONS ||--o{ INVOICES : generates
    SUBSCRIPTIONS }o--|| PRICES : uses
    PRICES }o--|| PRODUCTS : belongs_to
    SUBSCRIPTIONS }o--|| MRR_MONTHLY_SUMMARY : aggregates_to
```

## 7. Deployment Architecture (Future)

```mermaid
flowchart TB
    subgraph Production["Production Setup"]
        subgraph Scheduler["Automation"]
            CRON["Cloud Scheduler"]
            CF["Cloud Functions"]
        end
        
        subgraph Data["Data Layer"]
            STRIPE["Stripe API"]
            BQ["BigQuery"]
        end
        
        subgraph App["Application"]
            CR["Cloud Run"]
            API["Flask API"]
            REACT["React App"]
        end
        
        subgraph CDN["Delivery"]
            GCS["Cloud Storage"]
            LB["Load Balancer"]
        end
    end
    
    CRON -->|trigger| CF
    CF -->|extract| STRIPE
    CF -->|load| BQ
    BQ -->|query| API
    API -->|serve| CR
    REACT -->|hosted| GCS
    GCS --> LB
    CR --> LB
    LB -->|users| Internet["🌐 Internet"]
```

---

## Viewing These Diagrams

These diagrams use [Mermaid](https://mermaid.js.org/) syntax which renders automatically on:
- ✅ GitHub README/Markdown files
- ✅ VS Code with Mermaid extension
- ✅ Notion, Confluence, and other tools

To view locally, install the VS Code extension: `bierner.markdown-mermaid`
