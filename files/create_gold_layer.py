import pandas as pd
import boto3
from io import StringIO, BytesIO
from datetime import datetime

# Configuration
BUCKET_NAME = 'ad-campaign-optimizer-2026'

# Initialize AWS S3 client
s3_client = boto3.client('s3')

def get_latest_silver_file():
    """
    Get the most recent file from Silver layer
    """
    print(" Finding latest file in Silver layer...")
    
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix='silver/'
    )
    
    if 'Contents' not in response:
        print(" No files found in Silver layer!")
        return None
    
    # Get the latest file
    files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    latest_file = sorted(files, key=lambda x: x['LastModified'])[-1]
    
    print(f" Found: {latest_file['Key']}")
    return latest_file['Key']

def read_from_s3(s3_key):
    """
    Read CSV file from S3
    """
    print(f"Reading data from S3...")
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    
    print(f" Loaded {len(df)} rows with {len(df.columns)} columns")
    return df

def create_platform_performance(df):
    """
    Aggregate metrics by platform
    """
    print("\n Creating platform performance summary...")
    
    platform_stats = df.groupby('ad_platform').agg({
        'user_id': 'count',  # Total campaigns
        'impressions': 'sum',
        'clicks': 'sum',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'ctr': 'mean',
        'conversion_rate': 'mean',
        'cost_per_click': 'mean',
        'cost_per_conversion': 'mean',
        'roi_percentage': 'mean',
        'roas': 'mean',
        'profit': 'sum',
        'engagement_score': 'mean',
        'quality_score': 'mean'
    }).round(2)
    
    # Rename columns for clarity
    platform_stats.columns = [
        'total_campaigns',
        'total_impressions',
        'total_clicks',
        'total_conversions',
        'total_spend',
        'total_revenue',
        'avg_ctr',
        'avg_conversion_rate',
        'avg_cpc',
        'avg_cpa',
        'avg_roi',
        'avg_roas',
        'total_profit',
        'avg_engagement',
        'avg_quality_score'
    ]
    
    # Add recommendation
    platform_stats['budget_recommendation'] = platform_stats['avg_roi'].apply(
        lambda x: 'Increase Budget +30%' if x > 200 else
                  'Increase Budget +15%' if x > 100 else
                  'Maintain Budget' if x > 50 else
                  'Reduce Budget -20%'
    )
    
    # Calculate budget allocation suggestion
    total_roi = platform_stats['avg_roi'].sum()
    platform_stats['suggested_budget_allocation_%'] = (
        (platform_stats['avg_roi'] / total_roi) * 100
    ).round(1)
    
    platform_stats = platform_stats.reset_index()
    
    print(f" Created platform summary with {len(platform_stats)} platforms")
    return platform_stats

def create_demographic_insights(df):
    """
    Analyze performance by demographics
    """
    print("\n Creating demographic insights...")
    
    # Age group performance
    age_stats = df.groupby('age_group').agg({
        'user_id': 'count',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'roi_percentage': 'mean',
        'conversion_rate': 'mean'
    }).round(2)
    
    age_stats.columns = [
        'campaigns',
        'conversions',
        'spend',
        'revenue',
        'avg_roi',
        'avg_conversion_rate'
    ]
    age_stats = age_stats.reset_index()
    
    # Gender performance
    gender_stats = df.groupby('gender').agg({
        'user_id': 'count',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'roi_percentage': 'mean',
        'engagement_score': 'mean'
    }).round(2)
    
    gender_stats.columns = [
        'campaigns',
        'conversions',
        'spend',
        'revenue',
        'avg_roi',
        'avg_engagement'
    ]
    gender_stats = gender_stats.reset_index()
    
    # Location performance
    location_stats = df.groupby('location').agg({
        'user_id': 'count',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'roi_percentage': 'mean'
    }).round(2)
    
    location_stats.columns = [
        'campaigns',
        'conversions',
        'spend',
        'revenue',
        'avg_roi'
    ]
    location_stats = location_stats.reset_index()
    
    print(f" Created demographic summaries")
    return age_stats, gender_stats, location_stats

def create_device_performance(df):
    """
    Analyze performance by device type
    """
    print("\n Creating device performance analysis...")
    
    device_stats = df.groupby('device_type').agg({
        'user_id': 'count',
        'clicks': 'sum',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'ctr': 'mean',
        'conversion_rate': 'mean',
        'roi_percentage': 'mean'
    }).round(2)
    
    device_stats.columns = [
        'campaigns',
        'total_clicks',
        'total_conversions',
        'total_spend',
        'total_revenue',
        'avg_ctr',
        'avg_conversion_rate',
        'avg_roi'
    ]
    
    device_stats = device_stats.reset_index()
    
    print(f" Created device analysis with {len(device_stats)} device types")
    return device_stats

def create_time_analysis(df):
    """
    Analyze performance by day of week
    """
    print("\nCreating time-based analysis...")
    
    # Define day order
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    time_stats = df.groupby('day_of_week').agg({
        'user_id': 'count',
        'clicks': 'sum',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'roi_percentage': 'mean',
        'engagement_score': 'mean'
    }).round(2)
    
    time_stats.columns = [
        'campaigns',
        'total_clicks',
        'total_conversions',
        'total_spend',
        'total_revenue',
        'avg_roi',
        'avg_engagement'
    ]
    
    time_stats = time_stats.reset_index()
    
    # Sort by custom day order
    time_stats['day_order'] = time_stats['day_of_week'].apply(
        lambda x: day_order.index(x) if x in day_order else 7
    )
    time_stats = time_stats.sort_values('day_order').drop('day_order', axis=1)
    
    print(f"   Created time analysis")
    return time_stats

def create_ad_category_performance(df):
    """
    Analyze performance by ad category and type
    """
    print("\n Creating ad category analysis...")
    
    category_stats = df.groupby('ad_category').agg({
        'user_id': 'count',
        'conversion': 'sum',
        'amount_spent': 'sum',
        'conversion_value': 'sum',
        'roi_percentage': 'mean',
        'quality_score': 'mean'
    }).round(2)
    
    category_stats.columns = [
        'campaigns',
        'conversions',
        'spend',
        'revenue',
        'avg_roi',
        'avg_quality_score'
    ]
    category_stats = category_stats.reset_index()
    
    # Ad type analysis
    ad_type_stats = df.groupby('ad_type').agg({
        'user_id': 'count',
        'clicks': 'sum',
        'conversion': 'sum',
        'ctr': 'mean',
        'conversion_rate': 'mean',
        'roi_percentage': 'mean'
    }).round(2)
    
    ad_type_stats.columns = [
        'campaigns',
        'total_clicks',
        'total_conversions',
        'avg_ctr',
        'avg_conversion_rate',
        'avg_roi'
    ]
    ad_type_stats = ad_type_stats.reset_index()
    
    print(f" Created ad category and type analysis")
    return category_stats, ad_type_stats

def create_executive_summary(df, platform_stats):
    """
    Create executive-level KPIs
    """
    print("\nCreating executive summary...")
    
    summary = {
        'metric': [
            'Total Campaigns',
            'Total Ad Spend',
            'Total Revenue',
            'Total Profit',
            'Overall ROI %',
            'Average CTR %',
            'Average Conversion Rate %',
            'Total Conversions',
            'Cost Per Conversion',
            'Best Performing Platform',
            'Recommended Action'
        ],
        'value': [
            len(df),
            f"${df['amount_spent'].sum():,.2f}",
            f"${df['conversion_value'].sum():,.2f}",
            f"${df['profit'].sum():,.2f}",
            f"{df['roi_percentage'].mean():.2f}%",
            f"{df['ctr'].mean():.2f}%",
            f"{df['conversion_rate'].mean():.2f}%",
            int(df['conversion'].sum()),
            f"${df['cost_per_conversion'].mean():.2f}",
            platform_stats.loc[platform_stats['avg_roi'].idxmax(), 'ad_platform'],
            platform_stats.loc[platform_stats['avg_roi'].idxmax(), 'budget_recommendation']
        ]
    }
    
    summary_df = pd.DataFrame(summary)
    
    print(f" Created executive summary with {len(summary_df)} KPIs")
    return summary_df

def upload_to_s3_gold(df, filename):
    """
    Upload aggregated data to S3 Gold layer
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"gold/{filename}_{timestamp}.csv"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    
    return s3_key

def main():
    """
    Main Gold layer creation pipeline
    """
    print("Starting Gold Layer Creation...")
    print("=" * 70)
    
    # Step 1: Get latest Silver file
    silver_key = get_latest_silver_file()
    if not silver_key:
        return
    
    # Step 2: Read from S3
    df = read_from_s3(silver_key)
    
    # Step 3: Create all business aggregations
    platform_perf = create_platform_performance(df)
    age_stats, gender_stats, location_stats = create_demographic_insights(df)
    device_perf = create_device_performance(df)
    time_analysis = create_time_analysis(df)
    category_stats, ad_type_stats = create_ad_category_performance(df)
    exec_summary = create_executive_summary(df, platform_perf)
    
    # Step 4: Upload all to Gold layer
    print("\nUploading to S3 Gold layer...")
    
    files_uploaded = []
    files_uploaded.append(upload_to_s3_gold(platform_perf, 'platform_performance'))
    files_uploaded.append(upload_to_s3_gold(age_stats, 'age_group_performance'))
    files_uploaded.append(upload_to_s3_gold(gender_stats, 'gender_performance'))
    files_uploaded.append(upload_to_s3_gold(location_stats, 'location_performance'))
    files_uploaded.append(upload_to_s3_gold(device_perf, 'device_performance'))
    files_uploaded.append(upload_to_s3_gold(time_analysis, 'day_of_week_performance'))
    files_uploaded.append(upload_to_s3_gold(category_stats, 'category_performance'))
    files_uploaded.append(upload_to_s3_gold(ad_type_stats, 'ad_type_performance'))
    files_uploaded.append(upload_to_s3_gold(exec_summary, 'executive_summary'))
    
    print(f" Uploaded {len(files_uploaded)} files to Gold layer")
    
    # Step 5: Display key insights
    print("\n" + "=" * 70)
    print("KEY INSIGHTS & RECOMMENDATIONS")
    print("=" * 70)
    
    print("\n1. EXECUTIVE SUMMARY:")
    print(exec_summary.to_string(index=False))
    
    print("\n2. PLATFORM PERFORMANCE:")
    print(platform_perf[['ad_platform', 'avg_roi', 'total_profit', 
                         'budget_recommendation', 'suggested_budget_allocation_%']].to_string(index=False))
    
    print("\n3. TOP PERFORMING DEMOGRAPHICS:")
    print(f"   Best Age Group: {age_stats.loc[age_stats['avg_roi'].idxmax(), 'age_group']} "
          f"(ROI: {age_stats['avg_roi'].max():.2f}%)")
    print(f"   Best Gender: {gender_stats.loc[gender_stats['avg_roi'].idxmax(), 'gender']} "
          f"(ROI: {gender_stats['avg_roi'].max():.2f}%)")
    print(f"   Best Location: {location_stats.loc[location_stats['avg_roi'].idxmax(), 'location']} "
          f"(ROI: {location_stats['avg_roi'].max():.2f}%)")
    
    print("\n4. DEVICE INSIGHTS:")
    print(device_perf[['device_type', 'avg_roi', 'avg_conversion_rate']].to_string(index=False))
    
    print("\n5. BEST DAY TO RUN ADS:")
    best_day = time_analysis.loc[time_analysis['avg_roi'].idxmax()]
    print(f"   {best_day['day_of_week']} - ROI: {best_day['avg_roi']:.2f}% "
          f"| Conversions: {int(best_day['total_conversions'])}")
    
    print("\n" + "=" * 70)
    print("Gold Layer Complete!")
    print("All business-ready data stored in: s3://{}/gold/".format(BUCKET_NAME))
    print("=" * 70)

if __name__ == "__main__":
    main()
