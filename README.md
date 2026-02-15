# Social_media_ad_end_to_end_project
Social Media Ad Campaign ROI Optimizer (End-to-End Data Engineering Project)
This project is an end-to-end **data engineering & analytics pipeline** that helps a digital marketing team decide **where to place their ads (Facebook vs Instagram), when to run them, and which audience to target** to maximize ROI.

The pipeline ingests ad performance data, enriches it with cost and ROI metrics, stores it using a Medallion (Bronze–Silver–Gold) architecture on AWS, exposes the data via Athena, and visualizes insights in Power BI.

---

## 1. Problem Statement

An imaginary client runs paid ad campaigns on **Facebook and Instagram** but has no clear view of:

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


