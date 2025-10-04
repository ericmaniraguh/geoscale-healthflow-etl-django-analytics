# etl_app/schemas/table_schemas.py
"""PostgreSQL table schemas for health center lab data"""

import logging

logger = logging.getLogger(__name__)


class TableSchemas:
    """Manages PostgreSQL table schemas for different data types"""
    
    @staticmethod
    def get_raw_data_schema(table_name: str) -> str:
        """Create comprehensive table structure for raw health center data"""
        return f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER,
            month INTEGER,
            district VARCHAR(100),
            sector VARCHAR(100),
            health_center VARCHAR(200),
            cell VARCHAR(100),
            village VARCHAR(100),
            age INTEGER,
            age_group VARCHAR(50),
            gender VARCHAR(20),
            slide_status VARCHAR(200),
            test_result VARCHAR(50),
            is_positive BOOLEAN,
            case_origin VARCHAR(100),
            province VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
        );
        """
    
    @staticmethod
    def get_raw_data_indexes(table_name: str) -> str:
        """Create indexes for raw data table"""
        safe_name = table_name.replace('-', '_')[:50]
        return f"""
        CREATE INDEX idx_{safe_name}_unique_id ON {table_name}(unique_id);
        CREATE INDEX idx_{safe_name}_location ON {table_name}(district, sector, village);
        CREATE INDEX idx_{safe_name}_time ON {table_name}(year, month);
        CREATE INDEX idx_{safe_name}_result ON {table_name}(is_positive, test_result);
        """
    
    @staticmethod
    def get_yearly_stats_schema(table_name: str) -> str:
        """Get SQL schema for yearly statistics table"""
        return f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER,
            total_tests INTEGER,
            positive_cases INTEGER,
            negative_cases INTEGER,
            inconclusive_cases INTEGER,
            positivity_rate DECIMAL(5,2),
            negativity_rate DECIMAL(5,2),
            inconclusive_rate DECIMAL(5,2),
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
        );
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_unique_id ON {table_name}(unique_id);
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_year ON {table_name}(year);
        """
    
    @staticmethod
    def get_gender_pos_schema(table_name: str) -> str:
        """Get SQL schema for gender positivity table"""
        return f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER,
            gender VARCHAR(20),
            total_tests INTEGER,
            positive_cases INTEGER,
            negative_cases INTEGER,
            inconclusive_cases INTEGER,
            positivity_rate DECIMAL(5,2),
            negativity_rate DECIMAL(5,2),
            inconclusive_rate DECIMAL(5,2),
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
        );
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_unique_id ON {table_name}(unique_id);
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_lookup ON {table_name}(year, gender);
        """
    
    @staticmethod
    def get_village_pos_schema(table_name: str) -> str:
        """Get SQL schema for village positivity table"""
        return f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            village VARCHAR(100),
            year INTEGER,
            district VARCHAR(100),
            sector VARCHAR(100),
            total_tests INTEGER,
            positive_cases INTEGER,
            negative_cases INTEGER,
            positivity_rate DECIMAL(5,2),
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
        );
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_unique_id ON {table_name}(unique_id);
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_lookup ON {table_name}(village, year);
        """
    
    @staticmethod
    def get_monthly_pos_schema(table_name: str) -> str:
        """Get SQL schema for monthly positivity table"""
        return f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            year INTEGER,
            month INTEGER,
            month_name VARCHAR(20),
            total_tests INTEGER,
            positive_cases INTEGER,
            positivity_rate DECIMAL(5,2),
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
        );
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_unique_id ON {table_name}(unique_id);
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_lookup ON {table_name}(year, month);
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_positivity ON {table_name}(positivity_rate);
        """
    
    @staticmethod
    def get_summary_schema(table_name: str) -> str:
        """Get SQL schema for summary table"""
        return f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            unique_id VARCHAR(36) UNIQUE NOT NULL,
            total_records INTEGER,
            total_positive_cases INTEGER,
            total_negative_cases INTEGER,
            total_inconclusive_cases INTEGER,
            overall_pos_rate DECIMAL(5,2),
            year_range VARCHAR(50),
            years_covered TEXT,
            districts_count INTEGER,
            sectors_count INTEGER,
            villages_count INTEGER,
            districts_covered TEXT,
            sectors_covered TEXT,
            gender_breakdown TEXT,
            age_group_breakdown TEXT,
            filter_district VARCHAR(100),
            filter_sector VARCHAR(100),
            filter_years VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
        );
        CREATE INDEX idx_{table_name.replace('-', '_')[:50]}_unique_id ON {table_name}(unique_id);
        """