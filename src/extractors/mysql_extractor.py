"""
MySQL Extractor Module
======================
Extracts data from MySQL databases into pandas DataFrames.

Business Logic:
- MySQL is a common relational database for operational data
- We need to handle connection failures gracefully
- Support both full table extraction and custom queries
- Ensure connections are properly closed to prevent leaks
"""

import pandas as pd
from src.utils.db_connection import MySQLConnection
from typing import Optional


class MySQLExtractor:
    """
    Extracts data from MySQL databases.
    
    This extractor provides a simple interface to query MySQL
    and retrieve data as pandas DataFrames.
    """
    
    def __init__(self, config, logger):
        """
        Initialize MySQL extractor.
        
        Args:
            config (dict): MySQL source configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.db_connection = MySQLConnection(config, logger)
        self.table_name = config.get('table')
    
    def extract(self, table_name=None, query=None):
        """
        Extract data from MySQL database.
        
        Args:
            table_name (str, optional): Name of table to extract.
                                       Uses config default if None.
            query (str, optional): Custom SQL query.
                                  If provided, executes this instead of reading table.
        
        Returns:
            pd.DataFrame: Extracted data
        """
        # Use provided table name or default from config
        table = table_name or self.table_name
        
        if query:
            return self._extract_with_query(query)
        else:
            return self._extract_table(table)
    
    def _extract_table(self, table_name):
        """
        Extract entire table from MySQL.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            pd.DataFrame: Table data
        """
        self.logger.info(f"Extracting MySQL table: {table_name}")
        
        try:
            # Use the db_connection utility
            df = self.db_connection.read_table(table_name)
            
            rows, cols = df.shape
            self.logger.info(
                f"MySQL extraction successful: {rows} rows, {cols} columns from '{table_name}'"
            )
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to extract from MySQL table '{table_name}': {e}"
            self.logger.error(error_msg)
            raise
    
    def _extract_with_query(self, query):
        """
        Extract data using custom SQL query.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pd.DataFrame: Query results
        """
        self.logger.info(f"Extracting MySQL data with custom query")
        self.logger.debug(f"Query: {query}")
        
        try:
            # Use the db_connection utility with custom query
            df = self.db_connection.read_table(
                table_name=None,  # Not needed for custom query
                query=query
            )
            
            rows, cols = df.shape
            self.logger.info(
                f"MySQL query extraction successful: {rows} rows, {cols} columns"
            )
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to execute MySQL query: {e}"
            self.logger.error(error_msg)
            raise
    
    def test_connection(self):
        """
        Test the MySQL database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        self.logger.info("Testing MySQL connection...")
        return self.db_connection.test_connection()


# Example usage and testing
if __name__ == "__main__":
    from src.utils import ConfigLoader, get_logger
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        
        # Check if MySQL is enabled
        if not config_loader.get('sources.mysql.enabled'):
            print("\n" + "="*80)
            print("MySQL source is DISABLED in configuration")
            print("To test MySQL extractor:")
            print("1. Install MySQL database")
            print("2. Set 'enabled: true' in config/pipeline_config.yaml")
            print("3. Update database credentials")
            print("="*80)
        else:
            mysql_config = config_loader.get_source_config('mysql')
            log_config = config_loader.get_logging_config()
            
            # Set up logger
            logger = get_logger('mysql_extractor_test', log_config)
            
            # Create extractor
            extractor = MySQLExtractor(mysql_config, logger)
            
            # Test connection
            print("\n" + "="*80)
            print("MYSQL EXTRACTION TEST")
            print("="*80)
            
            if extractor.test_connection():
                # Extract data
                df = extractor.extract()
                print(f"\nShape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"\nFirst 5 rows:")
                print(df.head())
            else:
                print("Connection test failed!")
            
            print("="*80)
            
    except Exception as e:
        print(f"Test failed: {e}")