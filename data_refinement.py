#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import re
import json
import csv
from io import StringIO
from typing import Dict, List, Tuple

class DataRefinement:
    def __init__(self, data_path: str = "."):
        self.data_path = data_path
        self.merchants = []
        self.customers = []
        self.sales_data = []
        self.inventory_data = []
        self.business_customers = []
        self.processed_data = {}
        
    def load_data_files(self):
        print("Loading data files...")
        
        csv_files = glob.glob(f"{self.data_path}/*.csv")
        excel_files = glob.glob(f"{self.data_path}/*.xlsx")
        
        print(f"Found {len(csv_files)} CSV files and {len(excel_files)} Excel files")
        
        for file in csv_files:
            try:
                if "Customer" in file:
                    self._process_customer_file(file)
                elif "Revenue" in file or "Sales" in file:
                    self._process_sales_file(file)
            except Exception as e:
                print(f"Error processing {file}: {e}")
                
        for file in excel_files:
            try:
                if "customer_list" in file.lower():
                    self._process_business_customer_file(file)
                elif "inventory" in file.lower():
                    self._process_inventory_file(file)
            except Exception as e:
                print(f"Error processing {file}: {e}")
    
    def _process_customer_file(self, file_path: str):
        print(f"Processing customer file: {file_path}")
        try:
            df = pd.read_csv(file_path)
            df['file_source'] = file_path
            
            if 'Customer Since' in df.columns:
                df['Customer Since'] = pd.to_datetime(df['Customer Since'], errors='coerce')
                
                cutoff_date = datetime.now() - timedelta(days=30)
                def determine_status(date_val):
                    if pd.isna(date_val):
                        return 'Inactive'
                    try:
                        if hasattr(date_val, 'replace'):
                            clean_date = date_val.replace(tzinfo=None)
                        else:
                            clean_date = date_val
                        return 'Active' if clean_date > cutoff_date else 'Inactive'
                    except:
                        return 'Inactive'
                
                df['Status'] = df['Customer Since'].apply(determine_status)
                
                df['Has_Name'] = (df['First Name'].notna()) | (df['Last Name'].notna())
                df['Has_Phone'] = df['Phone Number'].notna()
                df['Has_Email'] = df['Email Address'].notna()
                df['Has_Address'] = df['Address Line 1'].notna()
                df['Profile_Complete'] = df['Has_Name'] & df['Has_Phone'] & df['Has_Email']
                
            else:
                df['Status'] = 'Unknown'
            
            self.customers.append(df)
            print(f"  Loaded {len(df)} customers from {file_path}")
            
        except Exception as e:
            print(f"Error in customer file processing: {e}")
    
    def _process_sales_file(self, file_path: str):
        print(f"Processing sales file: {file_path}")
        try:
            merchant_name = re.search(r'([^/\\]+)-Revenue', file_path)
            if merchant_name:
                merchant_name = merchant_name.group(1)
            else:
                merchant_name = "Unknown"
            
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            date_range = lines[1].strip('"')
            
            sales_data = {
                'merchant_name': merchant_name,
                'date_range': date_range,
                'file_source': file_path
            }
            
            for line in lines:
                if 'Gross Sales' in line and '$' in line:
                    sales_data['gross_sales'] = self._extract_currency(line)
                elif 'Net Sales' in line and '$' in line:
                    sales_data['net_sales'] = self._extract_currency(line)
                elif 'Gross Profit,' in line and 'Margin' not in line:
                    sales_data['gross_profit'] = self._extract_currency(line)
                elif 'Gross Profit Margin' in line:
                    sales_data['gross_profit_margin'] = self._extract_percentage(line)
                
            sales_data['top_selling_items'] = self._extract_top_items(lines, merchant_name)
            
            sales_data['last_activity'] = self._extract_date_from_range(date_range)
            cutoff_date = datetime.now() - timedelta(days=30)
            sales_data['status'] = 'Active' if sales_data.get('last_activity', datetime.min) > cutoff_date else 'Inactive'
            
            self.sales_data.append(sales_data)
            
        except Exception as e:
            print(f"Error in sales file processing: {e}")
    
    def _map_inventory_to_merchant(self, file_path: str) -> str:
        if 'inventory-export-v2' in file_path:
            return 'MARATHON LIQUORS'
        elif 'inventory-export-2' in file_path:
            return 'POKE HANA'
        elif 'inventory-export' in file_path and 'v2' not in file_path and '2' not in file_path:
            return "Anthony's Pizza & Pasta"
        else:
            return 'Unknown Merchant'
    
    def _process_inventory_file(self, file_path: str):
        print(f"Processing inventory file: {file_path}")
        try:
            df = pd.read_excel(file_path)
            
            merchant_name = self._map_inventory_to_merchant(file_path)
            print(f"  Mapping to merchant: {merchant_name}")
            
            inventory_summary = {
                'merchant_name': merchant_name,
                'file_source': file_path,
                'total_items': len(df),
                'revenue_items': len(df[df['Non-revenue item'] == 'No']) if 'Non-revenue item' in df.columns else len(df),
                'non_revenue_items': len(df[df['Non-revenue item'] == 'Yes']) if 'Non-revenue item' in df.columns else 0,
                'items_with_cost': len(df[df['Cost'].notna()]) if 'Cost' in df.columns else 0,
                'hidden_items': len(df[df['Hidden'] == 'Yes']) if 'Hidden' in df.columns else 0,
                'avg_price': df['Price'].mean() if 'Price' in df.columns and df['Price'].notna().sum() > 0 else 0,
                'total_inventory_value': df['Price'].sum() if 'Price' in df.columns else 0
            }
            
            self.inventory_data.append(inventory_summary)
            print(f"  Loaded {len(df)} inventory items for {merchant_name}")
            
        except Exception as e:
            print(f"Error in inventory file processing: {e}")
    
    def _process_business_customer_file(self, file_path: str):
        print(f"Processing business customer file: {file_path}")
        try:
            df = pd.read_excel(file_path)
            df['file_source'] = file_path
            
            if 'Registration Date' in df.columns:
                df['Registration Date'] = pd.to_datetime(df['Registration Date'], errors='coerce')
                
                cutoff_date = datetime.now() - timedelta(days=30)
                df['Is_Active'] = (df['Account Status'] == 'Live') & (df['MTD Volume'] > 0)
                
                df['Total_Volume'] = df['MTD Volume'].fillna(0) + df['Last Month Volume'].fillna(0)
                df['High_Volume'] = df['Total_Volume'] > df['Total_Volume'].quantile(0.75)
                
                volume_mean = df['Total_Volume'].mean()
                df['Volume_Category'] = df['Total_Volume'].apply(
                    lambda x: 'High' if x > volume_mean * 2 else 'Medium' if x > volume_mean * 0.5 else 'Low'
                )
                
            self.business_customers.append(df)
            print(f"  Loaded {len(df)} business customers")
            
        except Exception as e:
            print(f"Error in business customer file processing: {e}")
    
    def _extract_currency(self, line: str) -> float:
        match = re.search(r'\"?\$([0-9,]+\.?\d*)\"?', line)
        if match:
            return float(match.group(1).replace(',', ''))
        return 0.0
    
    def _extract_percentage(self, line: str) -> float:
        match = re.search(r'(\d+\.?\d*)%', line)
        if match:
            return float(match.group(1))
        return 0.0
    
    def _extract_date_from_range(self, date_range: str) -> datetime:
        try:
            dates = re.findall(r'(\w+ \d+, \d+)', date_range)
            if dates:
                return datetime.strptime(dates[-1], '%b %d, %Y')
        except:
            pass
        return datetime.now()
    
    def _extract_top_items(self, lines: list, merchant_name: str) -> list:
        items = []
        if 'MARATHON LIQUORS' in merchant_name:
            items = self._parse_marathon_items(lines)
        elif 'POKE HANA' in merchant_name:
            items = self._parse_poke_items(lines)
        elif "Anthony's Pizza" in merchant_name or 'Pizza' in merchant_name:
            items = self._parse_pizza_items(lines)
        else:
            pass
        
        items.sort(key=lambda x: x['gross_sales'], reverse=True)
        return items[:3]
    
    def _parse_marathon_items(self, lines: list) -> list:
        items = []
        
        start_idx = None
        for i, line in enumerate(lines):
            if 'Name,Gross Sales,Net Sales,Sold' in line:
                start_idx = i + 1
                break
        
        if start_idx is None:
            return items
        
        parsed_count = 0
        for line in lines[start_idx:]:
            line = line.strip()
            if not line:
                continue
            
            try:
                csv_reader = csv.reader(StringIO(line))
                parts = list(csv_reader)[0]
                
                if len(parts) >= 2:
                    item_name = parts[0].strip(' "')
                    gross_sales_str = parts[1].strip(' "') 
                    
                    if item_name and gross_sales_str:
                        if item_name.upper() == 'TOTAL':
                            continue
                            
                        sales_amount = self._extract_currency(gross_sales_str)
                        if sales_amount > 0:
                            items.append({
                                'name': item_name,
                                'gross_sales': sales_amount
                            })
                            parsed_count += 1
            except:
                continue
        
        return items
    
    def _parse_poke_items(self, lines: list) -> list:
        items = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('Total (') and ')' in line:
                
                try:
                    csv_reader = csv.reader(StringIO(line))
                    parts = list(csv_reader)[0]
                    
                    if len(parts) >= 3:
                        category_name = parts[0].strip(' "')
                        gross_sales_str = parts[2].strip(' "')
                        
                        if category_name.startswith('Total (') and category_name.endswith(')'):
                            clean_name = category_name[7:-1]
                            
                            sales_amount = self._extract_currency(gross_sales_str)
                            if sales_amount > 0:
                                items.append({
                                    'name': clean_name,
                                    'gross_sales': sales_amount
                                })
                except:
                    continue
        
        return items
    
    def _parse_pizza_items(self, lines: list) -> list:
        items = []
        
        for line in lines:
            line = line.strip()
            if not line or not line.startswith(','):
                continue
            
            if 'Pizza' not in line and 'pizza' not in line:
                continue
            
            parts = [part.strip(' "') for part in line.split(',')]
            if len(parts) >= 3:
                item_name = parts[1]
                gross_sales_str = parts[2]
                
                if item_name and ('Pizza' in item_name or 'pizza' in item_name):
                    sales_amount = self._extract_currency(gross_sales_str)
                    if sales_amount > 0:
                        items.append({
                            'name': item_name,
                            'gross_sales': sales_amount
                        })
        
        return items
    
    def _add_inventory_to_merchants(self):
        if not self.inventory_data or not self.sales_data:
            return
        
        for merchant in self.sales_data:
            merchant_name = merchant['merchant_name']
            
            merchant_inventory = [
                inv for inv in self.inventory_data 
                if inv['merchant_name'] == merchant_name
            ]
            
            if merchant_inventory:
                merchant['inventory_details'] = merchant_inventory[0]
            else:
                merchant['inventory_details'] = {
                    'merchant_name': merchant_name,
                    'file_source': 'No inventory file',
                    'total_items': 0,
                    'revenue_items': 0,
                    'non_revenue_items': 0,
                    'items_with_cost': 0,
                    'hidden_items': 0,
                    'avg_price': 0,
                    'total_inventory_value': 0
                }
    
    def generate_analytics(self):
        print("\nGenerating analytics...")
        
        self._add_inventory_to_merchants()
        
        analytics = {
            'summary': {},
            'merchants': {},
            'customers': {},
            'business_customers': {},
            'predictions': {}
        }
        
        if self.customers:
            all_customers = pd.concat(self.customers, ignore_index=True)
            
            analytics['customers'] = {
                'total_onboarded': len(all_customers),
                'active_customers': len(all_customers[all_customers['Status'] == 'Active']),
                'inactive_customers': len(all_customers[all_customers['Status'] == 'Inactive']),
                'customers_with_names': len(all_customers[all_customers.get('Has_Name', pd.Series([False] * len(all_customers)))]),
                'customers_with_phone': len(all_customers[all_customers.get('Has_Phone', pd.Series([False] * len(all_customers)))]),
                'customers_with_email': len(all_customers[all_customers.get('Has_Email', pd.Series([False] * len(all_customers)))]),
                'customers_with_address': len(all_customers[all_customers.get('Has_Address', pd.Series([False] * len(all_customers)))]),
                'profile_complete': len(all_customers[all_customers.get('Profile_Complete', pd.Series([False] * len(all_customers)))]),
                'recent_signups_30days': len(all_customers[
                    (all_customers['Customer Since'].notna()) & 
                    (all_customers['Customer Since'] > (datetime.now() - timedelta(days=30)))
                ]),
                'date_range': {
                    'earliest': all_customers['Customer Since'].min().strftime('%Y-%m-%d') if all_customers['Customer Since'].notna().any() else None,
                    'latest': all_customers['Customer Since'].max().strftime('%Y-%m-%d') if all_customers['Customer Since'].notna().any() else None
                },
                'engagement_rate': len(all_customers[all_customers['Status'] == 'Active']) / len(all_customers) * 100 if len(all_customers) > 0 else 0
            }
        else:
            analytics['customers'] = {
                'total_onboarded': 0,
                'active_customers': 0,
                'inactive_customers': 0,
                'customers_with_names': 0,
                'customers_with_phone': 0,
                'customers_with_email': 0,
                'customers_with_address': 0,
                'profile_complete': 0,
                'recent_signups_30days': 0,
                'date_range': {'earliest': None, 'latest': None},
                'engagement_rate': 0
            }
            
            
        if self.business_customers:
            all_business = pd.concat(self.business_customers, ignore_index=True)
            
            analytics['business_customers'] = {
                'total_business_accounts': len(all_business),
                'active_accounts': len(all_business[all_business.get('Is_Active', pd.Series([False] * len(all_business)))]),
                'live_accounts': len(all_business[all_business['Account Status'] == 'Live']) if 'Account Status' in all_business.columns else 0,
                'total_mtd_volume': all_business['MTD Volume'].sum() if 'MTD Volume' in all_business.columns else 0,
                'total_last_month_volume': all_business['Last Month Volume'].sum() if 'Last Month Volume' in all_business.columns else 0,
                'high_volume_accounts': len(all_business[all_business.get('High_Volume', pd.Series([False] * len(all_business)))]),
                'avg_volume_per_account': all_business['MTD Volume'].mean() if 'MTD Volume' in all_business.columns and len(all_business) > 0 else 0,
                'volume_categories': {
                    'high': len(all_business[all_business.get('Volume_Category', pd.Series(['Low'] * len(all_business))) == 'High']),
                    'medium': len(all_business[all_business.get('Volume_Category', pd.Series(['Low'] * len(all_business))) == 'Medium']),
                    'low': len(all_business[all_business.get('Volume_Category', pd.Series(['Low'] * len(all_business))) == 'Low'])
                },
                'top_3_business_customers': self._get_top_business_customers(all_business, 3)
            }
        else:
            analytics['business_customers'] = {
                'total_business_accounts': 0,
                'active_accounts': 0,
                'live_accounts': 0,
                'total_mtd_volume': 0,
                'total_last_month_volume': 0,
                'high_volume_accounts': 0,
                'avg_volume_per_account': 0,
                'volume_categories': {'high': 0, 'medium': 0, 'low': 0},
                'top_3_business_customers': []
            }
        
        if self.sales_data:
            analytics['merchants'] = {
                'total_merchants': len(self.sales_data),
                'active_merchants': len([m for m in self.sales_data if m.get('status') == 'Active']),
                'inactive_merchants': len([m for m in self.sales_data if m.get('status') == 'Inactive']),
                'total_gross_sales': sum([m.get('gross_sales', 0) for m in self.sales_data]),
                'total_net_sales': sum([m.get('net_sales', 0) for m in self.sales_data]),
                'average_profit_margin': np.mean([m.get('gross_profit_margin', 0) for m in self.sales_data if m.get('gross_profit_margin', 0) > 0]),
                'merchant_details': self.sales_data
            }
            
            sorted_merchants = sorted(self.sales_data, key=lambda x: x.get('gross_sales', 0), reverse=True)
            analytics['merchants']['top_3_merchants'] = sorted_merchants[:3]
        else:
            analytics['merchants'] = {
                'total_merchants': 0,
                'active_merchants': 0,
                'inactive_merchants': 0,
                'total_gross_sales': 0,
                'total_net_sales': 0,
                'average_profit_margin': 0,
                'merchant_details': [],
                'top_3_merchants': []
            }
        
        analytics['predictions'] = self._generate_predictions()
        
        analytics['summary'] = {
            'total_entities_onboarded': (
                analytics['customers'].get('total_onboarded', 0) + 
                analytics['merchants'].get('total_merchants', 0) +
                analytics['business_customers'].get('total_business_accounts', 0)
            ),
            'total_platform_volume': analytics['merchants'].get('total_gross_sales', 0) + analytics['business_customers'].get('total_mtd_volume', 0),
            'overall_active_rate': self._calculate_active_rate(analytics),
            'data_processing_date': datetime.now().isoformat(),
            'comprehensive_breakdown': {
                'individual_customers': analytics['customers'].get('total_onboarded', 0),
                'merchants': analytics['merchants'].get('total_merchants', 0),
                'business_customers': analytics['business_customers'].get('total_business_accounts', 0)
            }
        }
        
        self.processed_data = analytics
        return analytics
    
    def _generate_predictions(self) -> Dict:
        predictions = {
            'next_2_months': {},
            'same_period_next_year': {},
            'methodology': 'Linear trend extrapolation based on current data'
        }
        
        if not self.sales_data:
            return predictions
        
        total_sales = sum([m.get('gross_sales', 0) for m in self.sales_data])
        num_merchants = len(self.sales_data)
        
        monthly_avg = total_sales / max(num_merchants, 1) / 3
        growth_rate = 0.05
        
        month1_prediction = monthly_avg * (1 + growth_rate)
        month2_prediction = month1_prediction * (1 + growth_rate)
        
        predictions['next_2_months'] = {
            'month_1_forecast': round(month1_prediction, 2),
            'month_2_forecast': round(month2_prediction, 2),
            'total_2_months': round(month1_prediction + month2_prediction, 2)
        }
        
        annual_growth_rate = 0.15
        next_year_forecast = total_sales * (1 + annual_growth_rate)
        
        predictions['same_period_next_year'] = {
            'forecast': round(next_year_forecast, 2),
            'growth_projection': f"{annual_growth_rate * 100}%"
        }
        
        return predictions
    
    def _get_top_business_customers(self, business_df, limit: int = 3):
        if 'Total_Volume' in business_df.columns:
            top_customers = business_df.nlargest(limit, 'Total_Volume')
            return [{
                'business_name': row.get('Legal Business Name', 'Unknown'),
                'dba_name': row.get('DBA Name', ''),
                'customer_id': row.get('Customer ID', ''),
                'total_volume': row.get('Total_Volume', 0),
                'mtd_volume': row.get('MTD Volume', 0),
                'last_month_volume': row.get('Last Month Volume', 0),
                'account_status': row.get('Account Status', 'Unknown'),
                'volume_category': row.get('Volume_Category', 'Low')
            } for _, row in top_customers.iterrows()]
        return []

    def _calculate_active_rate(self, analytics: Dict) -> float:
        total_entities = (
            analytics['customers'].get('total_onboarded', 0) + 
            analytics['merchants'].get('total_merchants', 0) +
            analytics['business_customers'].get('total_business_accounts', 0)
        )
        active_entities = (
            analytics['customers'].get('active_customers', 0) + 
            analytics['merchants'].get('active_merchants', 0) +
            analytics['business_customers'].get('active_accounts', 0)
        )
        
        if total_entities == 0:
            return 0.0
        
        return round((active_entities / total_entities) * 100, 2)
    
    def save_refined_data(self, output_file: str = "refined_data.json"):
        print(f"\nSaving refined data to {output_file}")
        with open(output_file, 'w') as f:
            json.dump(self.processed_data, f, indent=2, default=str)
        print(f"Data saved successfully!")
    
    def print_summary(self):
        if not self.processed_data:
            print("No data processed yet. Run generate_analytics() first.")
            return
        
        print("\n" + "="*50)
        print("DATA REFINEMENT SUMMARY")
        print("="*50)
        
        summary = self.processed_data['summary']
        customers = self.processed_data['customers']
        merchants = self.processed_data['merchants']
        business_customers = self.processed_data['business_customers']
        predictions = self.processed_data['predictions']
        
        print(f"\n COMPREHENSIVE PLATFORM OVERVIEW:")
        print(f"   Total Entities Onboarded: {summary['total_entities_onboarded']:,}")
        print(f"   - Individual Customers: {summary['comprehensive_breakdown']['individual_customers']:,}")
        print(f"   - Business Customers: {summary['comprehensive_breakdown']['business_customers']:,}")
        print(f"   - Merchants: {summary['comprehensive_breakdown']['merchants']:,}")
        print(f"   Total Platform Volume: ${summary['total_platform_volume']:,.2f}")
        print(f"   Overall Active Rate: {summary['overall_active_rate']}%")
        
        print(f"\n INDIVIDUAL CUSTOMERS:")
        print(f"   Total Onboarded: {customers['total_onboarded']:,}")
        print(f"   Active: {customers['active_customers']:,}")
        print(f"   Inactive: {customers['inactive_customers']:,}")
        print(f"   With Names: {customers['customers_with_names']:,}")
        print(f"   With Phone: {customers['customers_with_phone']:,}")
        print(f"   With Email: {customers['customers_with_email']:,}")
        print(f"   Complete Profiles: {customers['profile_complete']:,}")
        print(f"   Recent Signups (30d): {customers['recent_signups_30days']:,}")
        print(f"   Engagement Rate: {customers['engagement_rate']:.1f}%")
        if customers['date_range']['earliest']:
            print(f"   Customer Range: {customers['date_range']['earliest']} to {customers['date_range']['latest']}")
        
        print(f"\n BUSINESS CUSTOMERS:")
        print(f"   Total Business Accounts: {business_customers['total_business_accounts']:,}")
        print(f"   Active Accounts: {business_customers['active_accounts']:,}")
        print(f"   Live Accounts: {business_customers['live_accounts']:,}")
        print(f"   MTD Volume: ${business_customers['total_mtd_volume']:,.2f}")
        print(f"   Last Month Volume: ${business_customers['total_last_month_volume']:,.2f}")
        print(f"   High Volume Accounts: {business_customers['high_volume_accounts']:,}")
        print(f"   Avg Volume per Account: ${business_customers['avg_volume_per_account']:,.2f}")
        
        print(f"\n TOP 3 BUSINESS CUSTOMERS BY VOLUME:")
        for i, customer in enumerate(business_customers['top_3_business_customers'], 1):
            print(f"   {i}. {customer['business_name']}: ${customer['total_volume']:,.2f}")
        
        print(f"\n MERCHANTS:")
        print(f"   Total Merchants: {merchants['total_merchants']}")
        print(f"   Active: {merchants['active_merchants']}")
        print(f"   Inactive: {merchants['inactive_merchants']}")
        print(f"   Avg Profit Margin: {merchants['average_profit_margin']:.2f}%")
        
        print(f"\n TOP 3 MERCHANTS BY REVENUE:")
        for i, merchant in enumerate(merchants['top_3_merchants'], 1):
            print(f"   {i}. {merchant['merchant_name']}: ${merchant.get('gross_sales', 0):,.2f}")
            if merchant.get('inventory_details'):
                inv = merchant['inventory_details']
                print(f"      Inventory: {inv.get('total_items', 0)} items, Value: ${inv.get('total_inventory_value', 0):,.2f}")
        
        print(f"\n PREDICTIONS:")
        print(f"   Next 2 Months Total: ${predictions['next_2_months']['total_2_months']:,.2f}")
        print(f"   Same Period Next Year: ${predictions['same_period_next_year']['forecast']:,.2f}")

def main():
    print("Starting Data Refinement Process...")
    
    refiner = DataRefinement()
    
    refiner.load_data_files()
    
    analytics = refiner.generate_analytics()
    
    refiner.print_summary()
    
    refiner.save_refined_data()
    
    print(f"\n Data refinement completed successfully!")
    return analytics

if __name__ == "__main__":
    main()