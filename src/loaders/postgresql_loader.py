"""
PostgreSQL Loader Module
========================
Loads data to PostgreSQL databases.

Business Logic:
- PostgreSQL is powerful for analytics and data warehousing
- OLAP (Online Analytical Processing) - optimized for complex queries
- Supports schemas for organizing data
- Auto-creates tables if they don't exist
"""

import pandas as pd
from src.utils.db_connection import PostgreSQLConnection


class PostgreSQLLoader:
    """
    Loads data to PostgreSQL databases.
    
    This loader writes DataFrames to PostgreSQL tables with
    automatic table creation and flexible load strategies.
    """
    
    def __init__(self, config, logger):
        """
        Initialize PostgreSQL loader.
        
        Args:
            config (dict): PostgreSQL destination configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.db_connection = PostgreSQLConnection(config, logger)
        self.table_name = config.get('table', 'data_table')
        self.schema = config.get('schema', 'public')
        self.if_exists = config.get('if_exists', 'replace')
        self.create_table = config.get('create_table', True)
    
    def load(self, df, table_name=None, schema=None, if_exists=None):
        """
        Load DataFrame to PostgreSQL table.
        
        Args:
            df (pd.DataFrame): Data to load
            table_name (str, optional): Table name. Uses config default if None.
            schema (str, optional): Schema name. Uses config default if None.
            if_exists (str, optional): Action if table exists ('fail', 'replace', 'append').
                                      Uses config default if None.
        
        Returns:
            int: Number of rows loaded
        """
        table = table_name or self.table_name
        schema_name = schema or self.schema
        action = if_exists or self.if_exists
        
        self.logger.info(
            f"Loading data to PostgreSQL: {schema_name}.{table} "
            f"(mode: {action})"
        )
        
        try:
            # Write to database
            self.db_connection.write_table(
                df=df,
                table_name=table,
                schema=schema_name,
                if_exists=action
            )
            
            rows_loaded = len(df)
            self.logger.info(
                f"Successfully loaded {rows_loaded} rows to "
                f"{schema_name}.{table}"
            )
            
            return rows_loaded
            
        except Exception as e:
            error_msg = f"Failed to load data to PostgreSQL: {e}"
            self.logger.error(error_msg)
            raise
    
    def test_connection(self):
        """
        Test the PostgreSQL connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        self.logger.info("Testing PostgreSQL connection...")
        return self.db_connection.test_connection()


# Example usage and testing
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from src.utils import ConfigLoader, get_logger
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        log_config = config_loader.get_logging_config()
        logger = get_logger('postgresql_loader_test', log_config)
        
        print("\n" + "="*80)
        print("POSTGRESQL LOADER TEST")
        print("="*80)
        
        # Check if PostgreSQL is enabled
        if not config_loader.get('destinations.postgresql_analytics.enabled'):
            print("\n⚠️  PostgreSQL destination is DISABLED in configuration")
            print("\nTo enable PostgreSQL loader:")
            print("1. Install PostgreSQL database")
            print("2. Set 'enabled: true' in config/pipeline_config.yaml")
            print("3. Update database credentials")
            print("\n✓ Loader code is ready - just enable when you have a database!")
        else:
            pg_config = config_loader.get_destination_config('postgresql_analytics')
            loader = PostgreSQLLoader(pg_config, logger)
            
            if loader.test_connection():
                print("\n✓ PostgreSQL connection successful!")
                print("✓ Ready to load data")
            else:
                print("\n✗ PostgreSQL connection failed")
                print("Check your database credentials in config/pipeline_config.yaml")
        
        print("\n" + "="*80 + "\n")
        
    except Exception as e:
        print(f"\nNote: {e}")
        print("\nThis is normal if you don't have PostgreSQL installed.")
        print("The loader is ready to use when you set up a database!")
        print("\n" + "="*80 + "\n")