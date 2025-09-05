#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob

def process_individual_customers():
    print("Processing individual customer data...")
    customer_files = glob.glob("Customers-*.csv")
    all_customers = []
    
    for file_path in customer_files:
        print(f"  Processing {file_path}")
        try:
            df = pd.read_csv(file_path)
            df['Customer Since'] = pd.to_datetime(df['Customer Since'], errors='coerce')
            cutoff_date = pd.Timestamp('2024-01-01')
            df['Status'] = df['Customer Since'].apply(
                lambda x: 'Active' if pd.notna(x) and x > cutoff_date else 'Inactive'
            )
            df['Has_Name'] = (df['First Name'].notna()) | (df['Last Name'].notna())
            df['Has_Phone'] = df['Phone Number'].notna()
            df['Has_Email'] = df['Email Address'].notna()
            df['Has_Address'] = df['Address Line 1'].notna()
            df['Profile_Complete'] = df['Has_Name'] & df['Has_Phone'] & df['Has_Email']
            
            all_customers.append(df)
            print(f"    Loaded {len(df)} customers")
            
        except Exception as e:
            print(f"    Error processing {file_path}: {e}")
    
    if all_customers:
        combined_df = pd.concat(all_customers, ignore_index=True)
        
        analytics = {
            'total_onboarded': len(combined_df),
            'active_customers': len(combined_df[combined_df['Status'] == 'Active']),
            'inactive_customers': len(combined_df[combined_df['Status'] == 'Inactive']),
            'customers_with_names': len(combined_df[combined_df['Has_Name']]),
            'customers_with_phone': len(combined_df[combined_df['Has_Phone']]),
            'customers_with_email': len(combined_df[combined_df['Has_Email']]),
            'customers_with_address': len(combined_df[combined_df['Has_Address']]),
            'profile_complete': len(combined_df[combined_df['Profile_Complete']]),
            'recent_signups_30days': len(combined_df[
                (combined_df['Customer Since'].notna()) & 
                (combined_df['Customer Since'] > (pd.Timestamp.now() - pd.Timedelta(days=30)))
            ]),
            'date_range': {
                'earliest': combined_df['Customer Since'].min().strftime('%Y-%m-%d') if combined_df['Customer Since'].notna().any() else None,
                'latest': combined_df['Customer Since'].max().strftime('%Y-%m-%d') if combined_df['Customer Since'].notna().any() else None
            },
            'engagement_rate': len(combined_df[combined_df['Status'] == 'Active']) / len(combined_df) * 100 if len(combined_df) > 0 else 0,
            'profile_completion_rate': len(combined_df[combined_df['Profile_Complete']]) / len(combined_df) * 100 if len(combined_df) > 0 else 0
        }
        
        print(f"\n INDIVIDUAL CUSTOMER ANALYTICS:")
        print(f"   Total Customers: {analytics['total_onboarded']:,}")
        print(f"   Active (2024+): {analytics['active_customers']:,}")
        print(f"   Inactive: {analytics['inactive_customers']:,}")
        print(f"   With Names: {analytics['customers_with_names']:,}")
        print(f"   With Phone: {analytics['customers_with_phone']:,}")
        print(f"   With Email: {analytics['customers_with_email']:,}")
        print(f"   Complete Profiles: {analytics['profile_complete']:,}")
        print(f"   Profile Completion Rate: {analytics['profile_completion_rate']:.1f}%")
        print(f"   Engagement Rate: {analytics['engagement_rate']:.1f}%")
        print(f"   Date Range: {analytics['date_range']['earliest']} to {analytics['date_range']['latest']}")
        
        return analytics
    
    return None

if __name__ == "__main__":
    result = process_individual_customers()
    if result:
        print(f"\nSuccessfully processed {result['total_onboarded']:,} individual customers!")
    else:
        print("\nFailed to process customer data")