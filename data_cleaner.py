#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import shutil
import glob
import re
from datetime import datetime, timedelta
import json
import logging

class DataCleaner:
    def __init__(self, data_path="./data", output_path="./data_cleaned", backup_path="./data_backup"):
        self.data_path = data_path
        self.output_path = output_path
        self.backup_path = backup_path
        
        self.cleaning_stats = {
            'files_processed': 0,
            'files_cleaned': 0,
            'total_rows_before': 0,
            'total_rows_after': 0,
            'errors': [],
            'cleaning_details': {}
        }
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_cleaning.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self._create_directories()
    
    def _create_directories(self):
        for path in [self.output_path, self.backup_path]:
            if not os.path.exists(path):
                os.makedirs(path)
                self.logger.info(f"Created directory: {path}")
    
    def backup_original_files(self):
        self.logger.info("Creating backup of original files...")
        csv_files = glob.glob(f"{self.data_path}/*.csv")
        excel_files = glob.glob(f"{self.data_path}/*.xlsx")
        
        for file_path in csv_files + excel_files:
            filename = os.path.basename(file_path)
            backup_path = os.path.join(self.backup_path, filename)
            
            if not os.path.exists(backup_path):
                shutil.copy2(file_path, backup_path)
                self.logger.info(f"Backed up: {filename}")
            else:
                self.logger.info(f"Backup already exists: {filename}")
    
    def clean_customer_data(self, df, file_path):
        original_count = len(df)
        key_fields = ['First Name', 'Last Name', 'Email Address', 'Phone Number']
        existing_key_fields = [field for field in key_fields if field in df.columns]
        
        if existing_key_fields:
            mask = df[existing_key_fields].notna().any(axis=1)
            df = df[mask]
        
        if 'Phone Number' in df.columns:
            def clean_phone(phone):
                if pd.isna(phone):
                    return phone
                phone_str = str(phone).strip()
                phone_digits = re.sub(r'[^\d]', '', phone_str)
                if len(phone_digits) < 10 or len(phone_digits) > 11:
                    return None
                return phone_str
            df['Phone Number'] = df['Phone Number'].apply(clean_phone)\
            
        if 'Email Address' in df.columns:
            def clean_email(email):
                if pd.isna(email):
                    return email
                email_str = str(email).strip().lower()
                if '@' not in email_str or '.' not in email_str:
                    return None
                if len(email_str) < 5 or email_str.count('@') != 1:
                    return None
                return email_str
            
            df['Email Address'] = df['Email Address'].apply(clean_email)
        
        for name_field in ['First Name', 'Last Name']:
            if name_field in df.columns:
                def clean_name(name):
                    if pd.isna(name):
                        return name
                    name_str = str(name).strip()
                    if len(name_str) < 2 or name_str.isdigit():
                        return None
                    return name_str.title()
                
                df[name_field] = df[name_field].apply(clean_name)
        
        if 'Customer Since' in df.columns:
            df['Customer Since'] = pd.to_datetime(df['Customer Since'], errors='coerce')
            future_mask = df['Customer Since'] > datetime.now()
            df.loc[future_mask, 'Customer Since'] = None
        
        if 'Email Address' in df.columns:
            df = df.drop_duplicates(subset=['Email Address'], keep='first')
        elif 'Phone Number' in df.columns:
            df = df.drop_duplicates(subset=['Phone Number'], keep='first')
        
        cleaned_count = len(df)
        return df, {
            'original_rows': original_count,
            'cleaned_rows': cleaned_count,
            'rows_removed': original_count - cleaned_count,
            'removal_rate': (original_count - cleaned_count) / original_count * 100
        }
    
    def clean_sales_data(self, df, file_path):
        original_count = len(df)
        if len(df.columns) > 1:
            def is_item_row(row):
                try:
                    second_col = str(row.iloc[1]).strip()
                    return '$' in second_col or second_col.replace('.', '').replace(',', '').isdigit()
                except:
                    return False
            item_mask = df.apply(is_item_row, axis=1)
            header_rows = df.head(10)
            item_rows = df[item_mask]
            if len(item_rows) > 0:
                df = pd.concat([header_rows, item_rows], ignore_index=True)
        
        cleaned_count = len(df)
        
        return df, {
            'original_rows': original_count,
            'cleaned_rows': cleaned_count,
            'rows_removed': original_count - cleaned_count,
            'removal_rate': (original_count - cleaned_count) / original_count * 100
        }
    
    def clean_business_data(self, df, file_path):
        original_count = len(df)
        
        if 'Legal Business Name' in df.columns:
            df = df[df['Legal Business Name'].notna() & (df['Legal Business Name'] != '')]
        
        volume_columns = ['MTD Volume', 'Last Month Volume', 'Total Volume']
        for col in volume_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'Registration Date' in df.columns:
            df['Registration Date'] = pd.to_datetime(df['Registration Date'], errors='coerce')
        
        if 'Legal Business Name' in df.columns:
            df = df.drop_duplicates(subset=['Legal Business Name'], keep='first')
        
        cleaned_count = len(df)
        
        return df, {
            'original_rows': original_count,
            'cleaned_rows': cleaned_count,
            'rows_removed': original_count - cleaned_count,
            'removal_rate': (original_count - cleaned_count) / original_count * 100
        }
    
    def clean_inventory_data(self, df, file_path):
        original_count = len(df)
        
        name_columns = ['Name', 'Item Name', 'Product Name']
        name_col = None
        for col in name_columns:
            if col in df.columns:
                name_col = col
                break
        
        if name_col:
            df = df[df[name_col].notna() & (df[name_col] != '')]
        
        price_columns = ['Price', 'Cost', 'Sale Price']
        for col in price_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df[(df[col] >= 0) | df[col].isna()]
        
        if name_col:
            df = df.drop_duplicates(subset=[name_col], keep='first')
        
        cleaned_count = len(df)
        
        return df, {
            'original_rows': original_count,
            'cleaned_rows': cleaned_count,
            'rows_removed': original_count - cleaned_count,
            'removal_rate': (original_count - cleaned_count) / original_count * 100
        }
    
    def determine_file_type(self, file_path):
        filename = os.path.basename(file_path).lower()
        
        if 'customer' in filename:
            return 'customer'
        elif 'revenue' in filename or 'sales' in filename:
            return 'sales'
        elif 'inventory' in filename:
            return 'inventory'
        elif 'business' in filename or 'customer_list' in filename:
            return 'business'
        else:
            return 'unknown'
    
    def clean_file(self, file_path):
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing: {filename}")
        
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            else:
                self.logger.warning(f"Unsupported file type: {filename}")
                return None
            
            file_type = self.determine_file_type(file_path)
            
            if file_type == 'customer':
                cleaned_df, stats = self.clean_customer_data(df.copy(), file_path)
            elif file_type == 'sales':
                cleaned_df, stats = self.clean_sales_data(df.copy(), file_path)
            elif file_type == 'business':
                cleaned_df, stats = self.clean_business_data(df.copy(), file_path)
            elif file_type == 'inventory':
                cleaned_df, stats = self.clean_inventory_data(df.copy(), file_path)
            else:
                original_count = len(df)
                cleaned_df = df.dropna(how='all')
                cleaned_count = len(cleaned_df)
                stats = {
                    'original_rows': original_count,
                    'cleaned_rows': cleaned_count,
                    'rows_removed': original_count - cleaned_count,
                    'removal_rate': (original_count - cleaned_count) / original_count * 100
                }
            
            output_file = os.path.join(self.output_path, filename)
            if file_path.endswith('.csv'):
                cleaned_df.to_csv(output_file, index=False)
            else:
                cleaned_df.to_excel(output_file, index=False)
            
            self.cleaning_stats['files_processed'] += 1
            self.cleaning_stats['files_cleaned'] += 1
            self.cleaning_stats['total_rows_before'] += stats['original_rows']
            self.cleaning_stats['total_rows_after'] += stats['cleaned_rows']
            self.cleaning_stats['cleaning_details'][filename] = {
                'file_type': file_type,
                **stats
            }
            
            self.logger.info(f"{filename}: {stats['original_rows']} → {stats['cleaned_rows']} rows ({stats['removal_rate']:.1f}% removed)")
            
            return cleaned_df
            
        except Exception as e:
            error_msg = f"Error processing {filename}: {str(e)}"
            self.logger.error(error_msg)
            self.cleaning_stats['errors'].append(error_msg)
            return None
    
    def clean_all_files(self):
        self.logger.info("🧹 Starting Data Cleaning Process")
        self.logger.info("=" * 50)
        
        self.backup_original_files()
        
        csv_files = glob.glob(f"{self.data_path}/*.csv")
        excel_files = glob.glob(f"{self.data_path}/*.xlsx")
        all_files = csv_files + excel_files
        
        self.logger.info(f"Found {len(all_files)} files to clean")
        
        for file_path in all_files:
            self.clean_file(file_path)
        
        self.generate_summary_report()
    
    def generate_summary_report(self):
        self.logger.info("\n" + "=" * 60)
        self.logger.info("DATA CLEANING SUMMARY REPORT")
        self.logger.info("=" * 60)
        
        stats = self.cleaning_stats
        
        self.logger.info(f"Files Processed: {stats['files_processed']}")
        self.logger.info(f"Files Successfully Cleaned: {stats['files_cleaned']}")
        self.logger.info(f"Files with Errors: {len(stats['errors'])}")
        self.logger.info(f"Total Rows Before: {stats['total_rows_before']:,}")
        self.logger.info(f"Total Rows After: {stats['total_rows_after']:,}")
        
        if stats['total_rows_before'] > 0:
            overall_removal_rate = ((stats['total_rows_before'] - stats['total_rows_after']) / 
                                  stats['total_rows_before'] * 100)
            self.logger.info(f"Overall Data Reduction: {overall_removal_rate:.2f}%")
        
        self.logger.info("\n DETAILED BREAKDOWN:")
        for filename, details in stats['cleaning_details'].items():
            self.logger.info(f"  {filename} ({details['file_type']}):")
            self.logger.info(f"    Before: {details['original_rows']:,} rows")
            self.logger.info(f"    After: {details['cleaned_rows']:,} rows")
            self.logger.info(f"    Removed: {details['rows_removed']:,} rows ({details['removal_rate']:.1f}%)")
        
        if stats['errors']:
            self.logger.info("\nERRORS ENCOUNTERED:")
            for error in stats['errors']:
                self.logger.info(f"  - {error}")
        
        report_data = {
            'cleaning_date': datetime.now().isoformat(),
            'summary': stats,
            'recommendations': self._generate_recommendations()
        }
        
        report_file = os.path.join(self.output_path, 'cleaning_report.json')
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        self.logger.info(f"\n💾 Detailed report saved to: {report_file}")
        self.logger.info(f"🗂️  Cleaned files available in: {self.output_path}")
        self.logger.info(f"💿 Original files backed up to: {self.backup_path}")
    
    def _generate_recommendations(self):
        recommendations = []
        
        if self.cleaning_stats['errors']:
            recommendations.append("Review and fix files that encountered errors during cleaning")
        
        total_removal_rate = 0
        if self.cleaning_stats['total_rows_before'] > 0:
            total_removal_rate = ((self.cleaning_stats['total_rows_before'] - 
                                 self.cleaning_stats['total_rows_after']) / 
                                self.cleaning_stats['total_rows_before'] * 100)
        
        if total_removal_rate > 20:
            recommendations.append("High data removal rate detected - review data collection processes")
        
        if total_removal_rate > 50:
            recommendations.append("CRITICAL: Over 50% of data removed - investigate data quality issues")
        
        for filename, details in self.cleaning_stats['cleaning_details'].items():
            if details['removal_rate'] > 30:
                recommendations.append(f"High removal rate in {filename} ({details['removal_rate']:.1f}%) - review data quality")
        
        if not recommendations:
            recommendations.append("Data quality looks good - minimal cleaning required")
        
        return recommendations

def main():
    """Main execution function"""
    print("🧹 Data Cleaning Script")
    print("=" * 40)
    
    cleaner = DataCleaner()
    
    if not os.path.exists(cleaner.data_path):
        print(f"Data directory not found: {cleaner.data_path}")
        print("Please ensure the data directory exists with your data files")
        return
    
    cleaner.clean_all_files()
    
    print("\nData cleaning completed!")
    print(f"Cleaned files saved to: {cleaner.output_path}")
    print(f"Backups created in: {cleaner.backup_path}")

if __name__ == "__main__":
    main()