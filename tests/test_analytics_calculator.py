import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from app.etl_app.services.analytics_calculator import AnalyticsCalculator

class TestAnalyticsCalculator:
    
    @pytest.fixture
    def calculator(self):
        return AnalyticsCalculator()

    @pytest.fixture
    def sample_df(self):
        data = {
            'year': [2023, 2023, 2023, 2024, 2024],
            'month': [1, 1, 2, 1, 1],
            'village': ['Village A', 'Village A', 'Village B', 'Village A', 'Village C'],
            'district': ['Dist1', 'Dist1', 'Dist1', 'Dist1', 'Dist1'],
            'sector': ['Sec1', 'Sec1', 'Sec1', 'Sec1', 'Sec1'],
            'gender': ['Male', 'Female', 'Male', 'Female', 'Male'],
            'age_group': ['15-24', '25-44', '5-14', '45-64', 'Under 5'],
            'test_result': ['Positive', 'Negative', 'Positive', 'Negative', 'Inconclusive'],
            'is_positive': [1, 0, 1, 0, 0]
        }
        return pd.DataFrame(data)

    def test_calculate_yearly_slide_status(self, calculator, sample_df):
        """Test yearly aggregation of slide status"""
        result = calculator._calculate_yearly_slide_status(sample_df)
        assert len(result) == 2
        
        # 2023: 3 tests, 2 pos, 1 neg
        y23 = next(r for r in result if r['year'] == 2023)
        assert y23['total_tests'] == 3
        assert y23['positive_cases'] == 2
        assert y23['negative_cases'] == 1
        assert y23['positivity_rate'] == 66.67

        # 2024: 2 tests, 0 pos, 1 neg, 1 inconcl
        y24 = next(r for r in result if r['year'] == 2024)
        assert y24['total_tests'] == 2
        assert y24['positive_cases'] == 0
        assert y24['inconclusive_cases'] == 1
        assert y24['positivity_rate'] == 0.0

    def test_calculate_gender_positivity_by_year(self, calculator, sample_df):
        """Test gender-based positivity rates"""
        result = calculator._calculate_gender_positivity_by_year(sample_df)
        
        # 2023 Male: 2 tests (1 pos, 1 pos) -> 100% positive
        male_23 = next(r for r in result if r['year'] == 2023 and r['gender'] == 'Male')
        assert male_23['total_tests'] == 2
        assert male_23['positive_cases'] == 2
        assert male_23['positivity_rate'] == 100.0

        # 2023 Female: 1 test (1 neg) -> 0% positive
        fem_23 = next(r for r in result if r['year'] == 2023 and r['gender'] == 'Female')
        assert fem_23['total_tests'] == 1
        assert fem_23['positive_cases'] == 0
        assert fem_23['positivity_rate'] == 0.0

    def test_calculate_village_positivity_by_year(self, calculator, sample_df):
        """Test village positivity rates"""
        result = calculator._calculate_village_positivity_by_year(sample_df)
        
        # Village A 2023: 2 records (1 pos, 1 neg) -> 50%
        va_23 = next(r for r in result if r['village'] == 'Village A' and r['year'] == 2023)
        assert va_23['total_tests'] == 2
        assert va_23['positive_cases'] == 1
        assert va_23['positivity_rate'] == 50.0

    def test_calculate_monthly_positivity(self, calculator, sample_df):
        """Test monthly positivity rates"""
        result = calculator._calculate_monthly_positivity(sample_df)
        
        # 2023 Month 1: 2 records (1 pos, 1 neg)
        m1_23 = next(r for r in result if r['year'] == 2023 and r['month'] == 1)
        assert m1_23['total_tests'] == 2
        assert m1_23['positivity_rate'] == 50.0
        
        # 2023 Month 2: 1 record (1 pos)
        m2_23 = next(r for r in result if r['year'] == 2023 and r['month'] == 2)
        assert m2_23['total_tests'] == 1
        assert m2_23['positivity_rate'] == 100.0

    def test_calculate_total_summary(self, calculator, sample_df):
        """Test overall summary statistics"""
        summary = calculator._calculate_total_summary(sample_df)
        
        assert summary['total_records'] == 5
        assert summary['total_positive_cases'] == 2
        assert summary['overall_pos_rate'] == 40.0 # 2/5 * 100 = 40.0
        assert summary['districts_count'] == 1
        assert summary['villages_count'] == 3
        # Check breakdown existence
        assert 'gender_breakdown' in summary
        assert 'age_group_breakdown' in summary
        
    def test_empty_dataframe(self, calculator):
        """Test handling of empty dataframe"""
        empty_df = pd.DataFrame()
        result = calculator.calculate_analytics(empty_df)
        assert result['yearly_slide_status'] == []
        assert result['gender_positivity_by_year'] == []
        assert result['village_positivity_by_year'] == []
        assert result['total_summary'] == {}
        assert result['monthly_positivity'] == []
