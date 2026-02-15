import pandas as pd
import boto3
from io import StringIO, BytesIO
from datetime import datetime

# Configuration
BUCKET_NAME = 'ad-campaign-optimizer-2026'

# Initialize AWS S3 client
s3_client = boto3.client('s3')

def get_latest_bronze_file():
    """
    Get the most recent file from Bronze layer
    """
    print("Finding latest file in Bronze layer...")
    
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix='bronze/'
    )
    
    if 'Contents' not in response:
        print("No files found in Bronze layer!")
        return None
    
    # Get the latest file
    files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    latest_file = sorted(files, key=lambda x: x['LastModified'])[-1]
    
    print(f"Found: {latest_file['Key']}")
    return latest_file['Key']

def read_from_s3(s3_key):
    """
    Read CSV file from S3
    """
    print(f" Reading data from S3...")
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    
    print(f" Loaded {len(df)} rows")
    return df

def clean_data(df):
    """
    Clean and validate data
    """
    print("\n Cleaning data...")
    
    initial_rows = len(df)
    
    # 1. Remove duplicates
    df = df.drop_duplicates(subset=['user_id', 'ad_id'])
    
    # 2. Remove invalid records (negative values, impossible metrics)
    df = df[df['age'] >= 18]
    df = df[df['age'] <= 100]
    df = df[df['impressions'] > 0]
    df = df[df['clicks'] <= df['impressions']]  # Clicks can't exceed impressions
    df = df[df['amount_spent'] >= 0]
    
    # 3. Standardize text fields
    df['gender'] = df['gender'].str.upper()
    df['location'] = df['location'].str.title()
    df['ad_platform'] = df['ad_platform'].str.title()
    
    removed_rows = initial_rows - len(df)
    print(f" Removed {removed_rows} invalid records")
    print(f" {len(df)} clean records remaining")
    
    return df

def calculate_metrics(df):
    """
    Calculate marketing KPIs and business metrics
    """
    print("\nCalculating marketing metrics...")
    
    # 1. Click-Through Rate (CTR)
    df['ctr'] = (df['clicks'] / df['impressions'] * 100).round(2)
    
    # 2. Conversion Rate
    df['conversion_rate'] = (df['conversion'] / df['clicks'].replace(0, 1) * 100).round(2)
    
    # 3. Cost Per Click (CPC)
    df['cost_per_click'] = (df['amount_spent'] / df['clicks'].replace(0, 1)).round(2)
    
    # 4. Cost Per Conversion (CPA - Cost Per Acquisition)
    df['cost_per_conversion'] = (df['amount_spent'] / df['conversion'].replace(0, 1)).round(2)
    
    # 5. Return on Ad Spend (ROAS)
    df['roas'] = (df['conversion_value'] / df['amount_spent'].replace(0, 1)).round(2)
    
    # 6. ROI Percentage
    df['roi_percentage'] = (
        (df['conversion_value'] - df['amount_spent']) / df['amount_spent'].replace(0, 1) * 100
    ).round(2)
    
    # 7. Profit/Loss
    df['profit'] = (df['conversion_value'] - df['amount_spent']).round(2)
    
    # 8. Engagement Quality Score (weighted formula)
    df['quality_score'] = (
        (df['ctr'] * 0.3) +
        (df['conversion_rate'] * 0.4) +
        (df['engagement_score'] * 100 * 0.3)
    ).round(2)
    
    print(f" Added 8 new calculated metrics")
    
    return df

def add_business_categories(df):
    """
    Add business-friendly categorizations
    """
    print("\n Adding business categories...")
    
    # 1. ROI Category
    df['roi_category'] = pd.cut(
        df['roi_percentage'],
        bins=[-float('inf'), 0, 100, 300, float('inf')],
        labels=['Loss', 'Low ROI', 'Good ROI', 'Excellent ROI']
    )
    
    # 2. Campaign Performance Category
    df['performance_category'] = pd.cut(
        df['quality_score'],
        bins=[0, 20, 40, 60, 100],
        labels=['Poor', 'Fair', 'Good', 'Excellent']
    )
    
    # 3. Age Group
    df['age_group'] = pd.cut(
        df['age'],
        bins=[0, 25, 35, 45, 55, 100],
        labels=['18-25', '26-35', '36-45', '46-55', '56+']
    )
    
    # 4. Spending Tier
    df['spending_tier'] = pd.cut(
        df['amount_spent'],
        bins=[0, 5, 10, 20, float('inf')],
        labels=['Low', 'Medium', 'High', 'Very High']
    )
    
    print(f"   ✓ Added 4 category columns")
    
    return df

def upload_to_s3_silver(df, filename):
    """
    Upload cleaned data to S3 Silver layer
    """
    print(f"\n Uploading to S3 Silver layer...")
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"silver/{filename}_{timestamp}.csv"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    
    print(f"Uploaded to: s3://{BUCKET_NAME}/{s3_key}")
    return s3_key

def main():
    """
    Main transformation pipeline
    """
    print("Starting Data Transformation Pipeline...")
    print("=" * 70)
    
    # Step 1: Get latest Bronze file
    bronze_key = get_latest_bronze_file()
    if not bronze_key:
        return
    
    # Step 2: Read from S3
    df = read_from_s3(bronze_key)
    
    # Step 3: Clean data
    df_clean = clean_data(df)
    
    # Step 4: Calculate metrics
    df_clean = calculate_metrics(df_clean)
    
    # Step 5: Add business categories
    df_clean = add_business_categories(df_clean)
    
    # Step 6: Show summary
    print(f"\nTransformation Summary:")
    print(f"   - Total records: {len(df_clean)}")
    print(f"   - Total columns: {len(df_clean.columns)} (added {len(df_clean.columns) - len(df)})")
    print(f"   - Average ROI: {df_clean['roi_percentage'].mean():.2f}%")
    print(f"   - Average CTR: {df_clean['ctr'].mean():.2f}%")
    print(f"   - Average Conversion Rate: {df_clean['conversion_rate'].mean():.2f}%")
    
    print(f"\nTop Performing Platform:")
    platform_roi = df_clean.groupby('ad_platform')['roi_percentage'].mean().sort_values(ascending=False)
    print(platform_roi)
    
    # Step 7: Upload to Silver layer
    silver_key = upload_to_s3_silver(df_clean, 'clean_campaigns')
    
    print("\n" + "=" * 70)
    print("Transformation Complete!")
    print(f" Clean data stored in Silver layer: {silver_key}")

if __name__ == "__main__":
    main()
