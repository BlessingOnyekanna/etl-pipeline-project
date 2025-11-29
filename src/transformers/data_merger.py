"""
Data Merger Module
==================
Merges data from multiple sources and standardizes formats.

Business Logic:
- Multiple data sources need to be combined into one dataset
- Data from different sources may have different formats
- Standardization ensures consistency across all data
- Clean, merged data is ready for analysis and loading
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional


class DataMerger:
    """
    Merges and standardizes data from multiple sources.
    
    This class combines DataFrames from different sources and
    applies standardization rules to ensure consistency.
    """
    
    def __init__(self, config, logger):
        """
        Initialize data merger.
        
        Args:
            config (dict): Transform configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
    
    def merge(self, dataframes_dict, how='outer'):
        """
        Merge multiple DataFrames into one.
        
        Args:
            dataframes_dict (dict): Dictionary of DataFrames {source_name: df}
            how (str): Type of merge ('inner', 'outer', 'left', 'right')
            
        Returns:
            pd.DataFrame: Merged DataFrame
        """
        if not dataframes_dict:
            raise ValueError("No DataFrames provided for merging")
        
        if len(dataframes_dict) == 1:
            # Only one source, return it standardized
            source_name, df = list(dataframes_dict.items())[0]
            self.logger.info(f"Single data source '{source_name}', applying standardization")
            return self.standardize(df)
        
        self.logger.info(f"Merging {len(dataframes_dict)} data sources")
        
        # Start with first DataFrame
        merged_df = None
        
        for source_name, df in dataframes_dict.items():
            self.logger.info(f"Processing source '{source_name}': {len(df)} rows")
            
            if merged_df is None:
                merged_df = df.copy()
            else:
                # Concatenate vertically (stack DataFrames)
                merged_df = pd.concat([merged_df, df], ignore_index=True, sort=False)
        
        self.logger.info(f"Merge complete: {len(merged_df)} total rows")
        
        # Apply standardization to merged data
        merged_df = self.standardize(merged_df)
        
        return merged_df
    
    def standardize(self, df):
        """
        Standardize data formats for consistency.
        
        Business Logic:
        - Customer names should be Title Case (Sarah Smith)
        - Order IDs should be uppercase with consistent prefix
        - Status codes should be uppercase
        - Email should be lowercase
        - Remove extra whitespace
        - Fix negative quantities
        
        Args:
            df (pd.DataFrame): Data to standardize
            
        Returns:
            pd.DataFrame: Standardized data
        """
        self.logger.info("Applying format standardization")
        df_std = df.copy()
        
        # 1. Standardize customer names (Title Case)
        if 'customer_name' in df_std.columns:
            df_std['customer_name'] = df_std['customer_name'].str.strip().str.title()
            self.logger.debug("Standardized customer names to Title Case")
        
        # 2. Standardize order IDs (uppercase, consistent format)
        if 'order_id' in df_std.columns:
            # Remove special characters like #, then add ORD- prefix if missing
            df_std['order_id'] = df_std['order_id'].astype(str).str.strip()
            df_std['order_id'] = df_std['order_id'].str.replace('#', '', regex=False)
            df_std['order_id'] = df_std['order_id'].str.replace('order', 'ORD-', regex=False, flags=2)
            df_std['order_id'] = df_std['order_id'].str.upper()
            
            # Ensure all have ORD- prefix
            mask = ~df_std['order_id'].str.startswith('ORD-')
            df_std.loc[mask, 'order_id'] = 'ORD-' + df_std.loc[mask, 'order_id']
            
            self.logger.debug("Standardized order IDs to ORD-XXXX format")
        
        # 3. Standardize email (lowercase, trim)
        if 'email' in df_std.columns:
            df_std['email'] = df_std['email'].str.strip().str.lower()
            self.logger.debug("Standardized emails to lowercase")
        
        # 4. Standardize status codes (uppercase, consistent abbreviations)
        if 'status' in df_std.columns:
            df_std['status'] = df_std['status'].str.strip().str.upper()
            
            # Fix common abbreviations/typos
            status_mapping = {
                'SHIPPD': 'SHIPPED',
                'DELIVERD': 'DELIVERED',
                'CNCLLD': 'CANCELLED',
                'COMPLETE': 'COMPLETED',
                'PNDNG': 'PENDING',
                'PROCESSING': 'PENDING'
            }
            df_std['status'] = df_std['status'].replace(status_mapping)
            self.logger.debug("Standardized status codes")
        
        # 5. Standardize category names (Title Case)
        if 'category' in df_std.columns:
            df_std['category'] = df_std['category'].str.strip().str.title()
            self.logger.debug("Standardized category names to Title Case")
        
        # 6. Standardize product names (Title Case)
        if 'product_name' in df_std.columns:
            df_std['product_name'] = df_std['product_name'].str.strip().str.title()
            self.logger.debug("Standardized product names to Title Case")
        
        # 7. Fix negative quantities (make absolute)
        if 'quantity' in df_std.columns:
            negative_count = (df_std['quantity'] < 0).sum()
            if negative_count > 0:
                df_std['quantity'] = df_std['quantity'].abs()
                self.logger.info(f"Fixed {negative_count} negative quantities by taking absolute value")
        
        # 8. Clean price column (remove $ signs, convert to float)
        if 'price' in df_std.columns:
            if df_std['price'].dtype == 'object':
                df_std['price'] = df_std['price'].astype(str).str.replace('$', '', regex=False)
                df_std['price'] = df_std['price'].str.replace(',', '', regex=False)
                df_std['price'] = pd.to_numeric(df_std['price'], errors='coerce')
                self.logger.debug("Converted price to numeric format")
        
        # 9. Remove tabs and extra whitespace from all string columns
        string_cols = df_std.select_dtypes(include=['object']).columns
        for col in string_cols:
            df_std[col] = df_std[col].astype(str).str.replace('\t', ' ', regex=False)
            df_std[col] = df_std[col].str.replace(r'\s+', ' ', regex=True)
            df_std[col] = df_std[col].str.strip()
        
        self.logger.info("Format standardization complete")
        
        return df_std
    
    def add_source_column(self, dataframes_dict):
        """
        Add a 'data_source' column to track where each record came from.
        
        Business Logic:
        - When merging multiple sources, need to track origin
        - Helps with debugging and auditing
        - Useful for data lineage
        
        Args:
            dataframes_dict (dict): Dictionary of DataFrames {source_name: df}
            
        Returns:
            dict: Updated dictionary with source column added
        """
        updated_dict = {}
        
        for source_name, df in dataframes_dict.items():
            df_copy = df.copy()
            df_copy['data_source'] = source_name
            updated_dict[source_name] = df_copy
            self.logger.debug(f"Added source column to '{source_name}'")
        
        return updated_dict


# Example usage and testing
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from src.utils import ConfigLoader, get_logger
    from src.extractors.csv_extractor import CSVExtractor
    from src.transformers.data_cleaner import DataCleaner
    from src.transformers.data_validator import DataValidator
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        transform_config = config_loader.get_transform_config()
        csv_config = config_loader.get_source_config('csv')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('data_merger_test', log_config)
        
        # Extract and clean data
        extractor = CSVExtractor(csv_config, logger)
        df_messy = extractor.extract()
        
        cleaner = DataCleaner(transform_config, logger)
        df_clean = cleaner.clean(df_messy, source_name="e-commerce")
        
        # Standardize data
        merger = DataMerger(transform_config, logger)
        
        print("\n" + "="*80)
        print("BEFORE STANDARDIZATION")
        print("="*80)
        print("\nSample data:")
        print(df_clean[['order_id', 'customer_name', 'email', 'status', 'quantity']].head(10))
        
        df_standardized = merger.standardize(df_clean)
        
        print("\n" + "="*80)
        print("AFTER STANDARDIZATION")
        print("="*80)
        print("\nSample data:")
        print(df_standardized[['order_id', 'customer_name', 'email', 'status', 'quantity']].head(10))
        
        # Validate standardized data
        validator = DataValidator(transform_config, logger)
        results = validator.validate(df_standardized, source_name="e-commerce_standardized")
        
        print("\n" + "="*80)
        print("QUALITY AFTER STANDARDIZATION")
        print("="*80)
        print(f"Quality Score: {results['quality_score']}/100")
        print(f"Completeness: {results['completeness']['completeness_percentage']}%")
        print(f"Uniqueness: {results['uniqueness']['uniqueness_percentage']}%")
        print(f"Validity: {results['validity']['validity_percentage']}%")
        print(f"Consistency: {results['consistency']['consistency_percentage']}%")
        print("\nUpdated report saved to: reports/data_quality_report.txt")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()