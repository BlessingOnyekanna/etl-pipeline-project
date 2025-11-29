"""
CSV Extractor Module
====================
Extracts data from CSV files into pandas DataFrames.

Business Logic:
- CSV files are one of the most common data sources
- We need to handle different delimiters (comma, tab, pipe, etc.)
- Encoding issues can occur with international characters
- Large files should be handled efficiently
"""

import pandas as pd
import os
from typing import Optional


class CSVExtractor:
    """
    Extracts data from CSV files.
    
    This extractor handles various CSV formats and encodings,
    making it easy to read data from file systems.
    """
    
    def __init__(self, config, logger):
        """
        Initialize CSV extractor.
        
        Args:
            config (dict): CSV source configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.file_path = config.get('file_path')
        self.delimiter = config.get('delimiter', ',')
        self.encoding = config.get('encoding', 'utf-8')
    
    def extract(self):
        """
        Extract data from CSV file into DataFrame.
        
        Returns:
            pd.DataFrame: Extracted data
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: If file can't be read
        """
        self.logger.info(f"Starting CSV extraction from: {self.file_path}")
        
        # Check if file exists
        if not os.path.exists(self.file_path):
            error_msg = f"CSV file not found: {self.file_path}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            # Read CSV file
            df = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding
            )
            
            # Log success
            rows, cols = df.shape
            self.logger.info(f"CSV extraction successful: {rows} rows, {cols} columns")
            
            # Show a preview of the data (first 3 rows)
            self.logger.debug(f"Data preview:\n{df.head(3)}")
            
            return df
            
        except UnicodeDecodeError:
            # Handle encoding issues
            self.logger.warning(
                f"Encoding '{self.encoding}' failed. Trying 'latin-1' encoding..."
            )
            try:
                df = pd.read_csv(
                    self.file_path,
                    delimiter=self.delimiter,
                    encoding='latin-1'
                )
                rows, cols = df.shape
                self.logger.info(
                    f"CSV extraction successful with 'latin-1': {rows} rows, {cols} columns"
                )
                return df
            except Exception as e:
                error_msg = f"Failed to read CSV with alternative encoding: {e}"
                self.logger.error(error_msg)
                raise
                
        except Exception as e:
            error_msg = f"Error reading CSV file: {e}"
            self.logger.error(error_msg)
            raise
    
    def validate_file(self):
        """
        Validate that the CSV file exists and is readable.
        
        Returns:
            bool: True if valid, False otherwise
        """
        if not os.path.exists(self.file_path):
            self.logger.error(f"CSV file does not exist: {self.file_path}")
            return False
        
        # Check if file is actually a CSV (basic check)
        if not self.file_path.endswith('.csv'):
            self.logger.warning(
                f"File doesn't have .csv extension: {self.file_path}"
            )
        
        # Check file size
        file_size_mb = os.path.getsize(self.file_path) / (1024 * 1024)
        self.logger.info(f"CSV file size: {file_size_mb:.2f} MB")
        
        return True


# Example usage and testing
if __name__ == "__main__":
    # This code runs only when you test this file directly
    # Not when it's imported by other files
    
    from src.utils import ConfigLoader, get_logger
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        csv_config = config_loader.get_source_config('csv')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('csv_extractor_test', log_config)
        
        # Create extractor
        extractor = CSVExtractor(csv_config, logger)
        
        # Validate file
        if extractor.validate_file():
            # Extract data
            df = extractor.extract()
            print("\n" + "="*80)
            print("CSV EXTRACTION TEST RESULTS")
            print("="*80)
            print(f"Shape: {df.shape}")
            print(f"\nColumns: {list(df.columns)}")
            print(f"\nFirst 5 rows:")
            print(df.head())
            print("="*80)
        else:
            print("File validation failed!")
            
    except Exception as e:
        print(f"Test failed: {e}")