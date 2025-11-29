"""
Data Validator Module
=====================
Validates data quality and generates comprehensive reports.

Business Logic:
- Data quality must be measured and reported
- Clients need proof that data is clean and trustworthy
- Quality reports help identify issues before they cause problems
- Automated validation saves manual inspection time
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List


class DataValidator:
    """
    Validates data quality and generates reports.
    
    This class assesses data quality across multiple dimensions
    and produces detailed reports for stakeholders.
    """
    
    def __init__(self, config, logger):
        """
        Initialize data validator.
        
        Args:
            config (dict): Transform configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.report_config = config.get('reporting', {})
    
    def validate(self, df, source_name="unknown"):
        """
        Validate data quality and generate report.
        
        Args:
            df (pd.DataFrame): Data to validate
            source_name (str): Name of data source
            
        Returns:
            dict: Validation results and quality metrics
        """
        self.logger.info(f"Starting data validation for '{source_name}'")
        
        validation_results = {
            'source_name': source_name,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'shape': df.shape,
            'completeness': self._check_completeness(df),
            'uniqueness': self._check_uniqueness(df),
            'validity': self._check_validity(df),
            'consistency': self._check_consistency(df),
            'quality_score': 0  # Will calculate at end
        }
        
        # Calculate overall quality score (0-100)
        validation_results['quality_score'] = self._calculate_quality_score(validation_results)
        
        self.logger.info(
            f"Validation complete for '{source_name}': "
            f"Quality Score = {validation_results['quality_score']}/100"
        )
        
        # Generate report if configured
        if self.report_config.get('enabled', True):
            self._generate_report(validation_results, df)
        
        return validation_results
    
    def _check_completeness(self, df):
        """
        Check data completeness (missing values).
        
        Args:
            df (pd.DataFrame): Data to check
            
        Returns:
            dict: Completeness metrics
        """
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        completeness_pct = ((total_cells - missing_cells) / total_cells) * 100
        
        missing_by_column = df.isnull().sum()
        columns_with_missing = missing_by_column[missing_by_column > 0].to_dict()
        
        return {
            'completeness_percentage': round(completeness_pct, 2),
            'total_missing': int(missing_cells),
            'columns_with_missing': columns_with_missing
        }
    
    def _check_uniqueness(self, df):
        """
        Check data uniqueness (duplicates).
        
        Args:
            df (pd.DataFrame): Data to check
            
        Returns:
            dict: Uniqueness metrics
        """
        total_rows = len(df)
        duplicate_rows = df.duplicated().sum()
        uniqueness_pct = ((total_rows - duplicate_rows) / total_rows) * 100 if total_rows > 0 else 100
        
        return {
            'uniqueness_percentage': round(uniqueness_pct, 2),
            'duplicate_rows': int(duplicate_rows),
            'unique_rows': int(total_rows - duplicate_rows)
        }
    
    def _check_validity(self, df):
        """
        Check data validity (data types, ranges).
        
        Args:
            df (pd.DataFrame): Data to check
            
        Returns:
            dict: Validity metrics
        """
        validity_issues = []
        
        # Check numeric columns for negative values where inappropriate
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if 'price' in col.lower() or 'quantity' in col.lower() or 'amount' in col.lower():
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    validity_issues.append({
                        'column': col,
                        'issue': 'negative_values',
                        'count': int(negative_count)
                    })
        
        # Check for unrealistic dates (future dates or too old)
        date_cols = df.select_dtypes(include=['datetime64']).columns
        for col in date_cols:
            future_dates = (df[col] > pd.Timestamp.now()).sum()
            if future_dates > 0:
                validity_issues.append({
                    'column': col,
                    'issue': 'future_dates',
                    'count': int(future_dates)
                })
            
            very_old = (df[col] < pd.Timestamp('1900-01-01')).sum()
            if very_old > 0:
                validity_issues.append({
                    'column': col,
                    'issue': 'unrealistic_old_dates',
                    'count': int(very_old)
                })
        
        validity_pct = 100 - (len(validity_issues) / len(df.columns) * 100) if len(df.columns) > 0 else 100
        
        return {
            'validity_percentage': round(max(0, validity_pct), 2),
            'issues': validity_issues
        }
    
    def _check_consistency(self, df):
        """
        Check data consistency (format standardization).
        
        Args:
            df (pd.DataFrame): Data to check
            
        Returns:
            dict: Consistency metrics
        """
        consistency_issues = []
        
        # Check string columns for inconsistent casing
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            if df[col].dtype == 'object':
                # Sample to check consistency
                sample = df[col].dropna().astype(str)
                if len(sample) > 0:
                    # Check if mixed case (some uppercase, some lowercase)
                    has_upper = sample.str.isupper().any()
                    has_lower = sample.str.islower().any()
                    has_title = sample.str.istitle().any()
                    
                    if sum([has_upper, has_lower, has_title]) > 1:
                        consistency_issues.append({
                            'column': col,
                            'issue': 'mixed_case_formats'
                        })
        
        consistency_pct = 100 - (len(consistency_issues) / len(df.columns) * 100) if len(df.columns) > 0 else 100
        
        return {
            'consistency_percentage': round(max(0, consistency_pct), 2),
            'issues': consistency_issues
        }
    
    def _calculate_quality_score(self, validation_results):
        """
        Calculate overall quality score (0-100).
        
        Business Logic:
        - Weighted average of completeness, uniqueness, validity, consistency
        - Completeness is most important (40%)
        - Uniqueness (30%), Validity (20%), Consistency (10%)
        
        Args:
            validation_results (dict): Validation metrics
            
        Returns:
            float: Quality score (0-100)
        """
        weights = {
            'completeness': 0.40,
            'uniqueness': 0.30,
            'validity': 0.20,
            'consistency': 0.10
        }
        
        score = (
            validation_results['completeness']['completeness_percentage'] * weights['completeness'] +
            validation_results['uniqueness']['uniqueness_percentage'] * weights['uniqueness'] +
            validation_results['validity']['validity_percentage'] * weights['validity'] +
            validation_results['consistency']['consistency_percentage'] * weights['consistency']
        )
        
        return round(score, 1)
    
    def _generate_report(self, validation_results, df):
        """
        Generate and save data quality report.
        
        Args:
            validation_results (dict): Validation metrics
            df (pd.DataFrame): Original data
        """
        report_path = self.report_config.get('output_path', 'reports/data_quality_report.txt')
        
        # Create reports directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("DATA QUALITY REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Source: {validation_results['source_name']}\n")
            f.write(f"Generated: {validation_results['timestamp']}\n")
            f.write(f"Dataset Shape: {validation_results['shape'][0]} rows x {validation_results['shape'][1]} columns\n\n")
            
            # Overall Quality Score
            score = validation_results['quality_score']
            status = "EXCELLENT" if score >= 90 else "GOOD" if score >= 75 else "FAIR" if score >= 60 else "POOR"
            f.write(f"OVERALL QUALITY SCORE: {score}/100 - {status}\n")
            f.write("=" * 80 + "\n\n")
            
            # Completeness
            comp = validation_results['completeness']
            f.write(f"1. COMPLETENESS: {comp['completeness_percentage']}%\n")
            f.write(f"   Total Missing Values: {comp['total_missing']}\n")
            if comp['columns_with_missing']:
                f.write("   Columns with Missing Values:\n")
                for col, count in comp['columns_with_missing'].items():
                    f.write(f"     - {col}: {count}\n")
            f.write("\n")
            
            # Uniqueness
            uniq = validation_results['uniqueness']
            f.write(f"2. UNIQUENESS: {uniq['uniqueness_percentage']}%\n")
            f.write(f"   Duplicate Rows: {uniq['duplicate_rows']}\n")
            f.write(f"   Unique Rows: {uniq['unique_rows']}\n\n")
            
            # Validity
            val = validation_results['validity']
            f.write(f"3. VALIDITY: {val['validity_percentage']}%\n")
            if val['issues']:
                f.write("   Issues Found:\n")
                for issue in val['issues']:
                    f.write(f"     - {issue['column']}: {issue['issue']} ({issue['count']} rows)\n")
            else:
                f.write("   No validity issues found\n")
            f.write("\n")
            
            # Consistency
            cons = validation_results['consistency']
            f.write(f"4. CONSISTENCY: {cons['consistency_percentage']}%\n")
            if cons['issues']:
                f.write("   Issues Found:\n")
                for issue in cons['issues']:
                    f.write(f"     - {issue['column']}: {issue['issue']}\n")
            else:
                f.write("   No consistency issues found\n")
            f.write("\n")
            
            # Statistics (if configured)
            if self.report_config.get('include_statistics', True):
                f.write("=" * 80 + "\n")
                f.write("STATISTICAL SUMMARY\n")
                f.write("=" * 80 + "\n\n")
                f.write(str(df.describe(include='all')))
                f.write("\n\n")
            
            # Sample Data (if configured)
            if self.report_config.get('include_samples', True):
                sample_size = self.report_config.get('sample_size', 5)
                f.write("=" * 80 + "\n")
                f.write(f"SAMPLE DATA (First {sample_size} rows)\n")
                f.write("=" * 80 + "\n\n")
                f.write(str(df.head(sample_size)))
                f.write("\n\n")
            
            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        self.logger.info(f"Data quality report saved to: {report_path}")


# Example usage and testing
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from src.utils import ConfigLoader, get_logger
    from src.extractors.csv_extractor import CSVExtractor
    from src.transformers.data_cleaner import DataCleaner
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        transform_config = config_loader.get_transform_config()
        csv_config = config_loader.get_source_config('csv')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('data_validator_test', log_config)
        
        # Extract and clean data
        extractor = CSVExtractor(csv_config, logger)
        df_messy = extractor.extract()
        
        cleaner = DataCleaner(transform_config, logger)
        df_clean = cleaner.clean(df_messy, source_name="e-commerce")
        
        # Validate cleaned data
        validator = DataValidator(transform_config, logger)
        
        print("\n" + "="*80)
        print("VALIDATING CLEANED DATA")
        print("="*80)
        
        results = validator.validate(df_clean, source_name="e-commerce_cleaned")
        
        print(f"\nQuality Score: {results['quality_score']}/100")
        print(f"Completeness: {results['completeness']['completeness_percentage']}%")
        print(f"Uniqueness: {results['uniqueness']['uniqueness_percentage']}%")
        print(f"Validity: {results['validity']['validity_percentage']}%")
        print(f"Consistency: {results['consistency']['consistency_percentage']}%")
        print("\nDetailed report saved to: reports/data_quality_report.txt")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()