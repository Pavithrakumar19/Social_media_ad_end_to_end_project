import boto3

BUCKET_NAME = 'ad-campaign-optimizer-2026'
s3_client = boto3.client('s3')

def reorganize_gold_files():
    """
    Move Gold layer files into separate folders by table type
    """
    print("Reorganizing Gold layer structure...")
    
    # List all files in gold/
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix='gold/'
    )
    
    if 'Contents' not in response:
        print(" No files found in gold/")
        return
    
    # File mapping: what substring to look for → which folder to put it in
    file_mappings = {
        'platform_performance': 'gold/platform_performance/',
        'executive_summary': 'gold/executive_summary/',
        'device_performance': 'gold/device_performance/',
        'day_of_week_performance': 'gold/day_of_week_performance/',
        'age_group_performance': 'gold/age_group_performance/',
        'gender_performance': 'gold/gender_performance/',
        'location_performance': 'gold/location_performance/',
        'category_performance': 'gold/category_performance/',
        'ad_type_performance': 'gold/ad_type_performance/'
    }
    
    for obj in response['Contents']:
        old_key = obj['Key']
        
        # Skip if it's already in a subfolder
        if old_key.count('/') > 1:
            continue
        
        # Find which table this file belongs to
        for pattern, new_folder in file_mappings.items():
            if pattern in old_key:
                new_key = old_key.replace('gold/', new_folder)
                
                # Copy to new location
                s3_client.copy_object(
                    Bucket=BUCKET_NAME,
                    CopySource={'Bucket': BUCKET_NAME, 'Key': old_key},
                    Key=new_key
                )
                
                # Delete old file
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_key)
                
                print(f"✅ Moved: {old_key} → {new_key}")
                break
    
    print("\n Reorganization complete!")

if __name__ == "__main__":
    reorganize_gold_files()
