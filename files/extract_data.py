import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# Configuration
BUCKET_NAME = 'ad-campaign-optimizer-2026'
CSV_FILE = 'social_media_ad_optimization.csv'

# Initialize AWS S3 client
s3_client = boto3.client('s3')

def load_local_data():
    """
    Load the CSV file from your computer
    """
    print("Loading data from local file...")
    df = pd.read_csv(CSV_FILE)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    return df

def add_cost_data(df):
    """
    Adding realistic cost data based on industry standards
    Different platforms have different costs per click (CPC)
    """
    print("\nAdding cost data based on industry benchmarks...")
    
    # Industry average Cost Per Click (CPC) by platform
    cpc_rates = {
        'Facebook': 1.72,    # $1.72 per click
        'Instagram': 1.20,   # $1.20 per click  
        'Twitter': 0.38,     # $0.38 per click
        'LinkedIn': 5.26     # $5.26 per click (most expensive)
    }
    
    # Calculate amount spent for each campaign
    df['amount_spent'] = df.apply(
        lambda row: row['clicks'] * cpc_rates.get(row['ad_platform'], 1.50),
        axis=1
    )
    
    # Round to 2 decimal places
    df['amount_spent'] = df['amount_spent'].round(2)
    
    # Calculate conversion value (assume each conversion is worth $50)
    df['conversion_value'] = df['conversion'] * 50.0
    
    print(f" Added cost columns (amount_spent, conversion_value)")
    return df

def upload_to_s3_bronze(df, filename):
    """
    Upload DataFrame to S3 Bronze layer (raw data)
    """
    print(f"\n Uploading to S3 Bronze layer...")
    
    # Convert DataFrame to CSV format
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"bronze/{filename}_{timestamp}.csv"
    
    try:
        # Upload to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        
        print(f"Uploaded to: s3://{BUCKET_NAME}/{s3_key}")
        return s3_key
    
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        print(f"   Make sure AWS credentials are configured correctly")
        return None

def main():
    """
    Main extraction pipeline
    """
    print(" Starting Data Extraction Pipeline...")
    print("=" * 70)
    
    # Step 1: Load data
    df = load_local_data()
    
    # Step 2: Add cost data
    df = add_cost_data(df)
    
    # Step 3: Show basic info
    print(f"\n Dataset Summary:")
    print(f"   - Total ad campaigns: {len(df)}")
    print(f"   - Platforms: {df['ad_platform'].unique().tolist()}")
    print(f"   - Total impressions: {df['impressions'].sum():,}")
    print(f"   - Total clicks: {df['clicks'].sum():,}")
    print(f"   - Total conversions: {df['conversion'].sum():,}")
    print(f"   - Total spend: ${df['amount_spent'].sum():,.2f}")
    print(f"   - Total conversion value: ${df['conversion_value'].sum():,.2f}")
    
    # Platform breakdown
    print(f"\n Platform Breakdown:")
    platform_summary = df.groupby('ad_platform').agg({
        'amount_spent': 'sum',
        'clicks': 'sum',
        'conversion': 'sum'
    }).round(2)
    print(platform_summary)
    
    # Step 4: Upload to S3 Bronze
    s3_path = upload_to_s3_bronze(df, 'raw_campaigns')
    
    if s3_path:
        print("\n" + "=" * 70)
        print("Extraction Complete!")
        print(f" Data stored in Bronze layer: {s3_path}")
    else:
        print("\n" + "=" * 70)
        print("Extraction completed but S3 upload failed.")
        print("Check your AWS credentials with: aws sts get-caller-identity")

if __name__ == "__main__":
    main()
