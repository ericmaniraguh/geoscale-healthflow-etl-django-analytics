# etl_app/services/analytics_calculator.py
"""Analytics calculations service for health center lab data"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Any


from ..utils.helpers import format_timestamp, generate_unique_id, sanitize_record

logger = logging.getLogger(__name__)


class AnalyticsCalculator:
    """Service for analytics calculations"""
    
    def __init__(self):
        pass
    
    def calculate_analytics(self, data_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate all required analytics with new requirements"""
        analytics = {}
        
        # 1. Yearly slide status statistics
        analytics['yearly_slide_status'] = self._calculate_yearly_slide_status(data_df)
        
        # 2. Gender positivity rate by year
        analytics['gender_positivity_by_year'] = self._calculate_gender_positivity_by_year(data_df)
        
        # 3. Village positivity rate (one entry per village per year)
        analytics['village_positivity_by_year'] = self._calculate_village_positivity_by_year(data_df)
        
        # 4. Total summary data
        analytics['total_summary'] = self._calculate_total_summary(data_df)
        
        # 5. Monthly positivity rates
        analytics['monthly_positivity'] = self._calculate_monthly_positivity(data_df)
        
        return analytics


    def format_timestamp(dt):
        if isinstance(dt, str):
            """Format timestamp to ISO 8601 format"""
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M")
        return dt.astimezone(timezone.utc).isoformat()
    

    def _calculate_monthly_positivity(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate monthly positivity rates across years"""
        if df.empty:
            return []
        
        results = []

        # Group by year and month
        for (year, month), group in df.groupby(['year', 'month']):
            if pd.notna(year) and pd.notna(month):
                total = len(group)
                positive = group['is_positive'].sum()
                
                # Positivity Rate Formula
                positivity_rate = round((positive / total * 100), 2) if total > 0 else 0

                month_names = {
                    1: 'January', 2: 'February', 3: 'March', 4: 'April',
                    5: 'May', 6: 'June', 7: 'July', 8: 'August',
                    9: 'September', 10: 'October', 11: 'November', 12: 'December'
                }

                results.append({
                    'unique_id': generate_unique_id(),
                    'year': int(year),
                    'month': int(month),
                    'month_name': month_names.get(int(month), f'Month {month}'),
                    'total_tests': int(total),
                    'positive_cases': int(positive),
                    'positivity_rate': positivity_rate,
                    'created_at': format_timestamp(datetime.now())
                })

        # Sort nicely by year and month
        return sorted(results, key=lambda x: (x['year'], x['month']))
    
   
    def _calculate_gender_positivity_by_year(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate gender-based positivity rates by year"""
        if df.empty:
            return []
        
        results = []
        
        # Group by year and gender
        for (year, gender), group in df.groupby(['year', 'gender']):
            if pd.notna(year) and pd.notna(gender):
                total = int(len(group))
                positive = int(group['is_positive'].sum())
                negative = int((group['test_result'] == 'Negative').sum())
                inconclusive = int(total - positive - negative)
                
                results.append({
                    'unique_id': generate_unique_id(),
                    'year': int(year),
                    'gender': str(gender),
                    'total_tests': total,
                    'positive_cases': positive,
                    'negative_cases': negative,
                    'inconclusive_cases': inconclusive, # Inconclusive cases mean results that cannot be confidently interpreted.
                    'positivity_rate': float(round((positive / total * 100), 2)) if total > 0 else 0.0,
                    'negativity_rate': float(round((negative / total * 100), 2)) if total > 0 else 0.0,
                    'inconclusive_rate': float(round((inconclusive / total * 100), 2)) if total > 0 else 0.0,
                    'created_at': format_timestamp(datetime.now())
                })
        
        return sorted(results, key=lambda x: (x['year'], x['gender']))

    
    def _calculate_village_positivity_by_year(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate village positivity rate - one entry per village per year"""
        if df.empty:
            return []
        
        results = []
        
        # Group by village and year (not by month)
        for (village, year), group in df.groupby(['village', 'year']):
            if pd.notna(village) and pd.notna(year) and village.strip():
                total = len(group)
                positive = group['is_positive'].sum()
                
                # Get additional village info from the first record
                sample_record = group.iloc[0]
                
        

            results.append(sanitize_record({
                    'unique_id': generate_unique_id(),
                    'village': str(village),
                    'year': int(year),
                    'district': str(sample_record.get('district', '')),
                    'sector': str(sample_record.get('sector', '')),
                    'total_tests': int(total),
                    'positive_cases': int(positive),
                    'negative_cases': int(total - positive),
                    'positivity_rate': round((positive / total * 100), 2) if total > 0 else 0.0,
                    'created_at': format_timestamp(datetime.now())
                }))
        return sorted(results, key=lambda x: (x['village'], x['year']))
    
    def _calculate_total_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate total summary statistics for the entire dataset"""
        if df.empty:
            return {}
        
        total_records = len(df)
        total_positive = df['is_positive'].sum()
        total_negative = len(df[df['test_result'] == 'Negative'])
        total_inconclusive = total_records - total_positive - total_negative
        
        # Get year range
        years = df['year'].dropna().unique()
        year_range = f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0]) if len(years) == 1 else "Unknown"
        
        # Get geographical coverage
        districts = df['district'].dropna().unique()
        sectors = df['sector'].dropna().unique()
        villages = df['village'].dropna().unique()
        
        # Gender breakdown
        gender_breakdown = df['gender'].value_counts().to_dict()
        
        # Age group breakdown
        age_group_breakdown = df['age_group'].value_counts().to_dict()
        
        summary = {
            'unique_id': generate_unique_id(),
            'total_records': int(total_records),
            'total_positive_cases': int(total_positive),
            'total_negative_cases': int(total_negative),
            'total_inconclusive_cases': int(total_inconclusive),
            'overall_pos_rate': round((total_positive / total_records * 100), 2) if total_records > 0 else 0,
            'year_range': year_range,
            'years_covered': sorted([int(y) for y in years if pd.notna(y)]),
            'districts_count': len(districts),
            'sectors_count': len(sectors),
            'villages_count': len(villages),
            'districts_covered': sorted([d for d in districts if d]),
            'sectors_covered': sorted([s for s in sectors if s]),
            'gender_breakdown': gender_breakdown,
            'age_group_breakdown': age_group_breakdown,
            'created_at': format_timestamp(datetime.now())
        }
        
        return summary
    
    def _calculate_yearly_slide_status(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate yearly slide status statistics"""
        if df.empty:
            return []
        
        results = []
        
        for year in df['year'].unique():
            if pd.notna(year):
                year_data = df[df['year'] == year]
                total = len(year_data)
                positive = year_data['is_positive'].sum()
                negative = len(year_data[year_data['test_result'] == 'Negative'])
                inconclusive = total - positive - negative
        
                results.append({
                    'unique_id': generate_unique_id(),
                    'year': int(year),
                    'total_tests': int(total),
                    'positive_cases': int(positive),
                    'negative_cases': int(negative),
                    'inconclusive_cases': int(inconclusive),
                    'positivity_rate': float(round((positive / total * 100), 2)) if total > 0 else 0.0,
                    'negativity_rate': float(round((negative / total * 100), 2)) if total > 0 else 0.0,
                    'inconclusive_rate': float(round((inconclusive / total * 100), 2)) if total > 0 else 0.0,
                    'created_at': format_timestamp(datetime.now())
                })

        
        return sorted(results, key=lambda x: x['year'])