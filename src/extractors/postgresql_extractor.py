"""
PostgreSQL Extractor Module
============================
Extracts data from PostgreSQL databases into pandas DataFrames.

Business Logic:
- PostgreSQL is a powerful relational database often used for analytics
- Supports schemas (namespaces for organizing tables)
- We need to handle connection failures gracefully
- Support both full table extraction and custom queries
"""

import pandas as pd
from src.utils.db_connection import PostgreSQLConnection
from typing import Optional


class PostgreSQLExtractor:
    """
    Extracts data from PostgreSQL databases.
    
    This extractor provides a simple interface to query PostgreSQL
    and retrieve data as pandas DataFrames.
    """
    
    def __init__(self, config, logger):
        """
        Initialize PostgreSQL extractor.
        
        Args:
            config (dict): PostgreSQL source configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.db_connection = PostgreSQLConnection(config, logger)
        self.table_name = config.get('table')
        self.schema = config.get('schema', 'public')
    
    def extract(self, table_name=None, schema=None, query=None):
        """
        Extract data from PostgreSQL database.
        
        Args:
            table_name (str, optional): Name of table to extract.
                                       Uses config default if None.
            schema (str, optional): Database schema. Uses config default if None.
            query (str, optional): Custom SQL query.
                                  If provided, executes this instead of reading table.
        
        Returns:
            pd.DataFrame: Extracted data
        """
        # Use provided values or defaults from config
        table = table_name or self.table_name
        schema_name = schema or self.schema
        
        if query:
            return self._extract_with_query(query)
        else:
            return self._extract_table(table, schema_name)
    
    def _extract_table(self, table_name, schema):
        """
        Extract entire table from PostgreSQL.
        
        Args:
            table_name (str): Name of the table
            schema (str): Database schema
            
        Returns:
            pd.DataFrame: Table data
        """
        self.logger.info(f"Extracting PostgreSQL table: {schema}.{table_name}")
        
        try:
            # Use the db_connection utility
            df = self.db_connection.read_table(
                table_name=table_name,
                schema=schema
            )
            
            rows, cols = df.shape
            self.logger.info(
                f"PostgreSQL extraction successful: {rows} rows, {cols} columns "
                f"from '{schema}.{table_name}'"
            )
            
            return df
            
        except Exception as e:
            error_msg = (
                f"Failed to extract from PostgreSQL table '{schema}.{table_name}': {e}"
            )
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
        self.logger.info(f"Extracting PostgreSQL data with custom query")
        self.logger.debug(f"Query: {query}")
        
        try:
            # Use the db_connection utility with custom query
            df = self.db_connection.read_table(
                table_name=None,  # Not needed for custom query
                query=query
            )
            
            rows, cols = df.shape
            self.logger.info(
                f"PostgreSQL query extraction successful: {rows} rows, {cols} columns"
            )
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to execute PostgreSQL query: {e}"
            self.logger.error(error_msg)
            raise
    
    def test_connection(self):
        """
        Test the PostgreSQL database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        self.logger.info("Testing PostgreSQL connection...")
        return self.db_connection.test_connection()


# Example usage and testing
if __name__ == "__main__":
    from src.utils import ConfigLoader, get_logger
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        
        # Check if PostgreSQL is enabled
        if not config_loader.get('sources.postgresql.enabled'):
            print("\n" + "="*80)
            print("PostgreSQL source is DISABLED in configuration")
            print("To test PostgreSQL extractor:")
            print("1. Install PostgreSQL database")
            print("2. Set 'enabled: true' in config/pipeline_config.yaml")
            print("3. Update database credentials")
            print("="*80)
        else:
            pg_config = config_loader.get_source_config('postgresql')
            log_config = config_loader.get_logging_config()
            
            # Set up logger
            logger = get_logger('postgresql_extractor_test', log_config)
            
            # Create extractor
            extractor = PostgreSQLExtractor(pg_config, logger)
            
            # Test connection
            print("\n" + "="*80)
            print("POSTGRESQL EXTRACTION TEST")
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