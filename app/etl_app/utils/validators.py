# etl_app/utils/validators.py
"""Input validation utilities for ETL operations"""

from typing import List, Dict, Any, Optional, Tuple


class ETLValidator:
    """Validator for ETL input parameters and data"""
    
    @staticmethod
    def validate_update_mode(update_mode: str) -> Tuple[bool, Optional[str]]:
        """Validate update mode parameter"""
        valid_modes = ['replace', 'append']
        if update_mode not in valid_modes:
            return False, f'update_mode must be one of {valid_modes}, received: {update_mode}'
        return True, None
    
    @staticmethod
    def validate_years(years_param: str, available_years: List[int]) -> Tuple[bool, Optional[str], Optional[List[int]]]:
        """Validate and parse years parameter"""
        if not years_param or years_param.lower() == 'all':
            return True, None, available_years
        
        try:
            years = [int(y.strip()) for y in years_param.split(',')]
            # Validate years against available years
            invalid_years = [y for y in years if y not in available_years]
            if invalid_years:
                return False, f'Invalid years: {invalid_years}. Available years: {available_years}', None
            return True, None, years
        except ValueError:
            return False, 'Invalid year format. Use comma-separated years or "all"', None
    
    @staticmethod
    def validate_filters(district: Optional[str] = None, sector: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Validate filter parameters"""
        # For now, basic validation - can be extended
        if district and len(district.strip()) == 0:
            return False, "District cannot be empty string"
        if sector and len(sector.strip()) == 0:
            return False, "Sector cannot be empty string"
        return True, None
    
    @staticmethod
    def validate_boolean_param(param_value: str, param_name: str) -> Tuple[bool, Optional[str], bool]:
        """Validate boolean parameters"""
        valid_values = ['true', 'false']
        param_lower = param_value.lower()
        if param_lower not in valid_values:
            return False, f'{param_name} must be "true" or "false", received: {param_value}', False
        return True, None, param_lower == 'true'
    
    @staticmethod
    def validate_table_prefix(table_prefix: str) -> Tuple[bool, Optional[str]]:
        """Validate table prefix parameter"""
        if not table_prefix:
            return False, "table_prefix cannot be empty"
        if not table_prefix.replace('_', '').isalnum():
            return False, "table_prefix can only contain letters, numbers, and underscores"
        if len(table_prefix) > 20:
            return False, "table_prefix cannot be longer than 20 characters"
        return True, None
    
    @staticmethod
    def validate_json_payload(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate JSON payload structure"""
        # Check for required fields or validate structure if needed
        # This is a placeholder for more complex validation
        if not isinstance(data, dict):
            return False, "Payload must be a JSON object"
        return True, None