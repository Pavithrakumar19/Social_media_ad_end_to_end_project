CREATE DATABASE IF NOT EXISTS ad_campaign_analytics
COMMENT 'Social Media Ad Campaign Performance Database'
LOCATION 's3://ad-campaign-optimizer-2026/';

--CREATING TABLE PLATFORM_PERFORMANCE
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
LOCATION 's3://ad-campaign-optimizer-2026/gold/platform_performance/'
TBLPROPERTIES ('skip.header.line.count'='1');

--TEST QUESRY:

SELECT * FROM ad_campaign_analytics.platform_performance;

#EXECUTIVE SUMMARY TABLE
CREATE EXTERNAL TABLE ad_campaign_analytics.executive_summary (
    metric STRING,
    value STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://ad-campaign-optimizer-2026/gold/executive_summary/'
TBLPROPERTIES ('skip.header.line.count'='1');

--DEVICE Performance
CREATE EXTERNAL TABLE ad_campaign_analytics.device_performance (
    device_type STRING,
    campaigns INT,
    total_clicks BIGINT,
    total_conversions INT,
    total_spend DECIMAL(10,2),
    total_revenue DECIMAL(10,2),
    avg_ctr DECIMAL(10,2),
    avg_conversion_rate DECIMAL(10,2),
    avg_roi DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://ad-campaign-optimizer-2026/gold/device_performance/'
TBLPROPERTIES ('skip.header.line.count'='1');


--DAY Performance
CREATE EXTERNAL TABLE ad_campaign_analytics.day_performance (
    day_of_week STRING,
    campaigns INT,
    total_clicks BIGINT,
    total_conversions INT,
    total_spend DECIMAL(10,2),
    total_revenue DECIMAL(10,2),
    avg_roi DECIMAL(10,2),
    avg_engagement DECIMAL(10,4)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://ad-campaign-optimizer-2026/gold/day_of_week_performance/'
TBLPROPERTIES ('skip.header.line.count'='1');



--AGE GROUP Performance
CREATE EXTERNAL TABLE ad_campaign_analytics.age_performance (
    age_group STRING,
    campaigns INT,
    conversions INT,
    spend DECIMAL(10,2),
    revenue DECIMAL(10,2),
    avg_roi DECIMAL(10,2),
    avg_conversion_rate DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://ad-campaign-optimizer-2026/gold/age_group_performance/'
TBLPROPERTIES ('skip.header.line.count'='1');


--VERIFYING ALL TABLES
-- Test 1: Platform data
SELECT ad_platform, avg_roi, budget_recommendation 
FROM ad_campaign_analytics.platform_performance;

-- Test 2: Device data  
SELECT device_type, avg_roi 
FROM ad_campaign_analytics.device_performance
ORDER BY avg_roi DESC;

-- Test 3: Day performance
SELECT day_of_week, total_conversions, avg_roi
FROM ad_campaign_analytics.day_performance
ORDER BY avg_roi DESC;

-- Test 4: Age groups
SELECT age_group, avg_roi
FROM ad_campaign_analytics.age_performance
ORDER BY avg_roi DESC;

-- Test 5: Executive summary
SELECT * FROM ad_campaign_analytics.executive_summary;


--FINAL CHECK FOR ALL TABLES
SHOW TABLES IN ad_campaign_analytics;
