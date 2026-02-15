# Social_media_ad_end_to_end_project
Social Media Ad Campaign ROI Optimizer (End-to-End Data Engineering Project)
This project is an end-to-end **data engineering & analytics pipeline** that helps a digital marketing team decide **where to place their ads (Facebook vs Instagram), when to run them, and which audience to target** to maximize ROI.

The pipeline ingests ad performance data, enriches it with cost and ROI metrics, stores it using a Medallion (Bronze–Silver–Gold) architecture on AWS, exposes the data via Athena, and visualizes insights in Power BI.

---

## 1. Problem Statement

An imaginary client runs paid ad campaigns on **Social media platforms** but has no clear view of:

- Which platform delivers the **best ROI**.
- Which **age groups**, **locations**, and **devices** convert best.
- Which **day of the week** they should run ads.
- How to **reallocate budget** between platforms to improve profit.

**Goal:** Build a data pipeline that produces **actionable recommendations** on:

- Which platform should get more budget.
- Which demographics to target.
- Which days/devices to prioritize.


##  System Architecture
```
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS Cloud (ap-south-1)                      │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │        S3 Bucket: s3://ad-campaign-optimizer-2026/             │ │
│  │                                                                │ │
│  │   bronze/                                                      │ │
│  │     └─ raw_campaigns_*.csv                                     │ │
│  │                           │                                    │ │
│  │   silver/               ▼                                      │ │
│  │     └─ clean_campaigns_*.csv                                   │ │
│  │                           │                                    │ │
│  │   gold/                 ▼                                      │ │
│  │     ├─ platform_performance/                                   │ │
│  │     ├─ demographics/                                           │ │
│  │     ├─ devices/                                                │ │
│  │     └─ executive_summary/                                      │ │
│  └────────────────────────┬───────────────────────────────────────┘ │
│                            │                                        │
│  ┌────────────────────────▼───────────────────────────────────────┐ │
│  │  AWS Athena - ad_campaign_analytics DB                         │ │
│  │  └─ External tables pointing to Gold layer                     │ │
│  └────────────────────────┬───────────────────────────────────────┘ │
└─────────────────────────────┼────────────────────────────────────────┘
                              │ ODBC 2.x Driver
                              ▼
                   ┌──────────────────────┐
                   │   Power BI Desktop   │
                   └──────────────────────┘
```
---
## Data Pipeline

### 1. Bronze – Ingestion & Cost Enrichment
**Script:** `extract_data.py`

- Reads `social_media_ad_optimization.csv`
- Adds realistic ad cost fields:
  - `amount_spent = clicks × platform-specific CPC`
  - `conversion_value = conversion × assumed value per conversion`
- Uploads enriched raw data to S3:
```
  s3://<your-bucket>/bronze/raw_campaigns_YYYYMMDD_HHMMSS.csv
```

### 2. Silver – Cleaning & KPI Engineering
**Script:** `transform_data.py`

- Reads latest Bronze file from S3
- **Cleans data:**
  - Removes duplicates and invalid rows (e.g., clicks > impressions, age out of range)
  - Normalizes text fields (ad_platform, gender, location)
- **Derives metrics:**
  - `ctr`, `conversion_rate`, `cost_per_click`, `cost_per_conversion`
  - `roas`, `roi_percentage`, `profit`
  - Composite `quality_score` from CTR, CVR, engagement
- **Adds business categories:**
  - `age_group`, `roi_category`, `performance_category`, `spending_tier`
- Writes clean, row-level data to:
```
  s3://<your-bucket>/silver/clean_campaigns_YYYYMMDD_HHMMSS.csv
```

### 3. Gold – Aggregations & Business Tables
**Script:** `create_gold_layer.py`

- Reads latest Silver file
- Creates multiple aggregated tables, written to S3:
```
  gold/platform_performance/platform_performance_*.csv
  gold/age_group_performance/age_group_performance_*.csv
  gold/gender_performance/gender_performance_*.csv
  gold/location_performance/location_performance_*.csv
  gold/device_performance/device_performance_*.csv
  gold/day_of_week_performance/day_of_week_performance_*.csv
  gold/category_performance/category_performance_*.csv
  gold/ad_type_performance/ad_type_performance_*.csv
  gold/executive_summary/executive_summary_*.csv
```

**Examples:**

- **platform_performance:**
  - Spend, revenue, profit, CTR, CVR, CPC, CPA, ROAS, ROI per platform
  - `budget_recommendation` (e.g., "Increase Budget +30%")
  - `suggested_budget_allocation_pct` (e.g., 64.3% to Instagram)

- **age_performance, gender_performance, location_performance:**
  - Conversions, spend, revenue, average ROI/CVR per segment

- **device_performance:**
  - Performance by device_type (Desktop, Mobile, Tablet)

- **day_of_week_performance:**
  - ROI and conversions by day_of_week

- **executive_summary:**
  - Total campaigns, spend, revenue, profit
  - Overall ROI %, CTR %, conversion rate
  - Best platform and recommended action

---

##  Athena Schema (Gold Layer)

**Database:** `ad_campaign_analytics`

**Example DDL for platform performance:**
```sql
CREATE DATABASE IF NOT EXISTS ad_campaign_analytics
LOCATION 's3://<your-bucket>/';

CREATE EXTERNAL TABLE ad_campaign_analytics.platform_performance (
    ad_platform STRING,
    total_campaigns INT,
    total_impressions BIGINT,
    total_clicks BIGINT,
    total_conversions INT,
    total_spend DECIMAL(10,2),
    total_revenue DECIMAL(10,2),
    avg_ctr DECIMAL(10,2),
    avg_conversion_rate DECIMAL(10,2),
    avg_cpc DECIMAL(10,2),
    avg_cpa DECIMAL(10,2),
    avg_roi DECIMAL(10,2),
    avg_roas DECIMAL(10,2),
    total_profit DECIMAL(10,2),
    avg_engagement DECIMAL(10,4),
    avg_quality_score DECIMAL(10,2),
    budget_recommendation STRING,
    suggested_budget_allocation_pct DECIMAL(5,1)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<your-bucket>/gold/platform_performance/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

Similar external tables exist for age, gender, location, device, day-of-week, and executive summary.

---

##  Power BI Dashboard

Power BI connects to Athena via the **Amazon Athena ODBC 2.x driver**:

- **DSN:** AthenaAdCampaign
- **Region:** ap-south-1
- **Output:** `s3://<your-bucket>/athena-results/`
- **Auth:** IAM credentials
## Key Insights (Sample Run)

- **Overall ROI:** ~511% (every $1 spent returns ~$5.11)
- **Platform:** Instagram ROI ~654% vs Facebook ~364% → shift budget towards Instagram
- **Age:** 26–35 age group yields highest ROI
- **Device:** Desktop slightly outperforms Mobile and Tablet on ROI
- **Timing:** Wednesday has best ROI and strong conversion count

---

##  How to Run Locally

### Prerequisites

- Python 3.9+
- AWS account (Free Tier)
- IAM user with at least:
  - `AmazonS3FullAccess`
  - `AmazonAthenaFullAccess`
- AWS CLI configured (`aws configure`)
- Power BI Desktop (Windows)

### Setup
```bash
git clone https://github.com/<your-username>/ad-campaign-optimizer.git
cd ad-campaign-optimizer

python -m venv venv
venv\Scripts\activate  # Windows
# or: source venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
```

Set your config (bucket name, region, etc.) inside the Python scripts.

### Run Pipeline
```bash
# 1. Bronze: ingest CSV and add cost fields
python extract_data.py

# 2. Silver: clean + engineer metrics
python transform_data.py

# 3. Gold: create aggregated business tables
python create_gold_layer.py
```

**Then:**

1. In Athena, run DDL scripts to create tables on the Gold paths
2. In Power BI, connect via ODBC to Athena and build the dashboarda ODBC to Athena and build the dashboard.

