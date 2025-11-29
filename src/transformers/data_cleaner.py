"""
Data Cleaner Module
===================
Cleans and standardizes data from various sources.

Business Logic:
- Real-world data is messy: missing values, duplicates, inconsistent formats
- Data quality issues lead to bad analysis and wrong decisions
- Automated cleaning saves time and ensures consistency
- Different strategies work for different data types
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List


class DataCleaner:
    """
    Cleans and standardizes data.
    
    This class handles common data quality issues like missing values,
    duplicates, outliers, and data type inconsistencies.
    """
    
    def __init__(self, config, logger):
        """
        Initialize data cleaner.
        
        Args:
            config (dict): Transform configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.quality_config = config.get('quality_checks', {})
        self.missing_config = config.get('missing_values', {})
        self.outlier_config = config.get('outliers', {})
    
    def clean(self, df, source_name="unknown"):
        """
        Apply all cleaning operations to a DataFrame.
        
        Args:
            df (pd.DataFrame): Data to clean
            source_name (str): Name of data source (for logging)
            
        Returns:
            pd.DataFrame: Cleaned data
        """
        self.logger.info(f"Starting data cleaning for '{source_name}'")
        original_rows = len(df)
        
        # Make a copy to avoid modifying original
        df_clean = df.copy()
        
        # 1. Remove duplicates
        if self.quality_config.get('remove_duplicates', True):
            df_clean = self._remove_duplicates(df_clean)
        
        # 2. Handle missing values
        if self.quality_config.get('handle_missing_values', True):
            df_clean = self._handle_missing_values(df_clean)
        
        # 3. Detect and handle outliers
        if self.quality_config.get('detect_outliers', True):
            df_clean = self._handle_outliers(df_clean)
        
        # 4. Validate and convert data types
        if self.quality_config.get('validate_data_types', True):
            df_clean = self._validate_data_types(df_clean)
        
        # Log summary
        final_rows = len(df_clean)
        rows_removed = original_rows - final_rows
        self.logger.info(
            f"Cleaning complete for '{source_name}': "
            f"{original_rows} → {final_rows} rows "
            f"({rows_removed} removed, {final_rows/original_rows*100:.1f}% retained)"
        )
        
        return df_clean
    
    def _remove_duplicates(self, df):
        """
        Remove duplicate rows from DataFrame.
        
        Args:
            df (pd.DataFrame): Input data
            
        Returns:
            pd.DataFrame: Data without duplicates
        """
        initial_count = len(df)
    
        # Convert any unhashable columns (dicts, lists) to strings
        # This is common with API data that has nested structures
        df_temp = df.copy()
        for col in df_temp.columns:
            # Check if column contains unhashable types
            if df_temp[col].apply(lambda x: isinstance(x, (dict, list))).any():
                self.logger.debug(f"Converting column '{col}' with nested data to string for deduplication")
                df_temp[col] = df_temp[col].astype(str)
        
        # Now we can safely drop duplicates
        df_clean = df_temp.drop_duplicates()
        duplicates_removed = initial_count - len(df_clean)
        
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate rows")
        else:
            self.logger.info("No duplicate rows found")
        
        return df_clean
    
    def _handle_missing_values(self, df):
        """
        Handle missing values in DataFrame.
        
        Business Logic:
        - Numeric columns: Fill with mean/median/zero or drop
        - Categorical columns: Fill with mode/unknown or drop
        - Drop columns if too many values are missing (>threshold)
        
        Args:
            df (pd.DataFrame): Input data
            
        Returns:
            pd.DataFrame: Data with missing values handled
        """
        df_clean = df.copy()
        
        # Get strategies from config
        numeric_strategy = self.missing_config.get('numeric_strategy', 'mean')
        categorical_strategy = self.missing_config.get('categorical_strategy', 'mode')
        threshold = self.missing_config.get('threshold', 0.5)
        
        # Identify numeric and categorical columns
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        categorical_cols = df_clean.select_dtypes(exclude=[np.number]).columns
        
        # Check for columns with too many missing values
        missing_pct = df_clean.isnull().sum() / len(df_clean)
        cols_to_drop = missing_pct[missing_pct > threshold].index.tolist()
        
        if cols_to_drop:
            self.logger.warning(
                f"Dropping columns with >{threshold*100}% missing: {cols_to_drop}"
            )
            df_clean = df_clean.drop(columns=cols_to_drop)
            # Update column lists
            numeric_cols = [col for col in numeric_cols if col not in cols_to_drop]
            categorical_cols = [col for col in categorical_cols if col not in cols_to_drop]
        
        # Handle numeric columns
        for col in numeric_cols:
            missing_count = df_clean[col].isnull().sum()
            if missing_count > 0:
                if numeric_strategy == 'mean':
                    fill_value = df_clean[col].mean()
                    df_clean[col].fillna(fill_value, inplace=True)
                    self.logger.debug(f"Filled {missing_count} missing values in '{col}' with mean: {fill_value:.2f}")
                elif numeric_strategy == 'median':
                    fill_value = df_clean[col].median()
                    df_clean[col].fillna(fill_value, inplace=True)
                    self.logger.debug(f"Filled {missing_count} missing values in '{col}' with median: {fill_value:.2f}")
                elif numeric_strategy == 'zero':
                    df_clean[col].fillna(0, inplace=True)
                    self.logger.debug(f"Filled {missing_count} missing values in '{col}' with 0")
                elif numeric_strategy == 'drop':
                    df_clean = df_clean.dropna(subset=[col])
                    self.logger.debug(f"Dropped {missing_count} rows with missing values in '{col}'")
        
        # Handle categorical columns
        for col in categorical_cols:
            missing_count = df_clean[col].isnull().sum()
            if missing_count > 0:
                if categorical_strategy == 'mode':
                    if not df_clean[col].mode().empty:
                        fill_value = df_clean[col].mode()[0]
                        df_clean[col].fillna(fill_value, inplace=True)
                        self.logger.debug(f"Filled {missing_count} missing values in '{col}' with mode: {fill_value}")
                    else:
                        df_clean[col].fillna('Unknown', inplace=True)
                        self.logger.debug(f"Filled {missing_count} missing values in '{col}' with 'Unknown'")
                elif categorical_strategy == 'unknown':
                    df_clean[col].fillna('Unknown', inplace=True)
                    self.logger.debug(f"Filled {missing_count} missing values in '{col}' with 'Unknown'")
                elif categorical_strategy == 'drop':
                    df_clean = df_clean.dropna(subset=[col])
                    self.logger.debug(f"Dropped {missing_count} rows with missing values in '{col}'")
        
        total_filled = df.isnull().sum().sum() - df_clean.isnull().sum().sum()
        self.logger.info(f"Handled {total_filled} missing values")
        
        return df_clean
    
    def _handle_outliers(self, df):
        """
        Detect and handle outliers in numeric columns.
        
        Business Logic:
        - Outliers can skew analysis and ML models
        - IQR method: Values beyond Q1-1.5*IQR or Q3+1.5*IQR
        - Z-score method: Values >3 standard deviations from mean
        - Actions: cap (limit to threshold), remove, or flag
        
        Args:
            df (pd.DataFrame): Input data
            
        Returns:
            pd.DataFrame: Data with outliers handled
        """
        df_clean = df.copy()
        
        method = self.outlier_config.get('method', 'iqr')
        threshold = self.outlier_config.get('threshold', 1.5)
        action = self.outlier_config.get('action', 'cap')
        
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        
        outlier_count = 0
        
        for col in numeric_cols:
            if method == 'iqr':
                # IQR method
                Q1 = df_clean[col].quantile(0.25)
                Q3 = df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                outliers = (df_clean[col] < lower_bound) | (df_clean[col] > upper_bound)
                
            elif method == 'zscore':
                # Z-score method
                mean = df_clean[col].mean()
                std = df_clean[col].std()
                z_scores = np.abs((df_clean[col] - mean) / std)
                outliers = z_scores > threshold
            
            else:
                self.logger.warning(f"Unknown outlier method: {method}")
                continue
            
            col_outliers = outliers.sum()
            if col_outliers > 0:
                outlier_count += col_outliers
                
                if action == 'cap':
                    # Cap values to bounds
                    if method == 'iqr':
                        df_clean.loc[df_clean[col] < lower_bound, col] = lower_bound
                        df_clean.loc[df_clean[col] > upper_bound, col] = upper_bound
                        self.logger.debug(f"Capped {col_outliers} outliers in '{col}' to [{lower_bound:.2f}, {upper_bound:.2f}]")
                    
                elif action == 'remove':
                    # Remove rows with outliers
                    df_clean = df_clean[~outliers]
                    self.logger.debug(f"Removed {col_outliers} outlier rows in '{col}'")
                
                elif action == 'flag':
                    # Add a flag column
                    flag_col = f'{col}_outlier_flag'
                    df_clean[flag_col] = outliers
                    self.logger.debug(f"Flagged {col_outliers} outliers in '{col}'")
        
        if outlier_count > 0:
            self.logger.info(f"Handled {outlier_count} outliers using '{method}' method with action '{action}'")
        else:
            self.logger.info("No outliers detected")
        
        return df_clean
    
    def _validate_data_types(self, df):
        """
        Validate and convert data types.
        
        Business Logic:
        - Dates stored as strings should be converted to datetime
        - Numbers stored as strings should be converted to numeric
        - Proper data types improve performance and enable operations
        
        Args:
            df (pd.DataFrame): Input data
            
        Returns:
            pd.DataFrame: Data with corrected types
        """
        df_clean = df.copy()
        
        if not self.config.get('type_conversions', {}).get('auto_detect', True):
            return df_clean
        
        conversions_made = 0
        
        for col in df_clean.columns:
            # Skip if already numeric
            if pd.api.types.is_numeric_dtype(df_clean[col]):
                continue
            
            # Try to convert to datetime
            if df_clean[col].dtype == 'object':
                # Check if column looks like dates
                try:
                    # Try parsing as dates
                    sample = df_clean[col].dropna().head(10)
                    if len(sample) > 0:
                        pd.to_datetime(sample, errors='raise')
                        # If successful, convert whole column
                        df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                        self.logger.debug(f"Converted '{col}' to datetime")
                        conversions_made += 1
                        continue
                except:
                    pass
                
                # Try to convert to numeric
                try:
                    converted = pd.to_numeric(df_clean[col], errors='coerce')
                    # Only convert if we don't lose too much data
                    if converted.notna().sum() / len(df_clean) > 0.9:
                        df_clean[col] = converted
                        self.logger.debug(f"Converted '{col}' to numeric")
                        conversions_made += 1
                except:
                    pass
        
        if conversions_made > 0:
            self.logger.info(f"Converted {conversions_made} columns to appropriate data types")
        
        return df_clean


# Example usage and testing
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from src.utils import ConfigLoader, get_logger
    from src.extractors.csv_extractor import CSVExtractor
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        transform_config = config_loader.get_transform_config()
        csv_config = config_loader.get_source_config('csv')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('data_cleaner_test', log_config)
        
        # Extract messy data
        extractor = CSVExtractor(csv_config, logger)
        df_messy = extractor.extract()
        
        print("\n" + "="*80)
        print("BEFORE CLEANING")
        print("="*80)
        print(f"Shape: {df_messy.shape}")
        print(f"Missing values: {df_messy.isnull().sum().sum()}")
        print(f"Duplicates: {df_messy.duplicated().sum()}")
        print(f"\nData types:\n{df_messy.dtypes}")
        
        # Clean the data
        cleaner = DataCleaner(transform_config, logger)
        df_clean = cleaner.clean(df_messy, source_name="e-commerce")
        
        print("\n" + "="*80)
        print("AFTER CLEANING")
        print("="*80)
        print(f"Shape: {df_clean.shape}")
        print(f"Missing values: {df_clean.isnull().sum().sum()}")
        print(f"Duplicates: {df_clean.duplicated().sum()}")
        print(f"\nData types:\n{df_clean.dtypes}")
        print("\n" + "="*80)
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()