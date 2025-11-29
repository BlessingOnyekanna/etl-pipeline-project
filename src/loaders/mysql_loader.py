"""
MySQL Loader Module
===================
Loads data to MySQL databases.

Business Logic:
- MySQL is popular for operational databases and backups
- OLTP (Online Transaction Processing) - optimized for fast inserts
- Good for application backends
- Auto-creates tables if they don't exist
"""

import pandas as pd
from src.utils.db_connection import MySQLConnection


class MySQLLoader:
    """
    Loads data to MySQL databases.
    
    This loader writes DataFrames to MySQL tables with
    automatic table creation and flexible load strategies.
    """
    
    def __init__(self, config, logger):
        """
        Initialize MySQL loader.
        
        Args:
            config (dict): MySQL destination configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.db_connection = MySQLConnection(config, logger)
        self.table_name = config.get('table', 'data_table')
        self.if_exists = config.get('if_exists', 'append')
        self.create_table = config.get('create_table', True)
    
    def load(self, df, table_name=None, if_exists=None):
        """
        Load DataFrame to MySQL table.
        
        Args:
            df (pd.DataFrame): Data to load
            table_name (str, optional): Table name. Uses config default if None.
            if_exists (str, optional): Action if table exists ('fail', 'replace', 'append').
                                      Uses config default if None.
        
        Returns:
            int: Number of rows loaded
        """
        table = table_name or self.table_name
        action = if_exists or self.if_exists
        
        self.logger.info(
            f"Loading data to MySQL: {table} (mode: {action})"
        )
        
        try:
            # Write to database
            self.db_connection.write_table(
                df=df,
                table_name=table,
                if_exists=action
            )
            
            rows_loaded = len(df)
            self.logger.info(
                f"Successfully loaded {rows_loaded} rows to {table}"
            )
            
            return rows_loaded
            
        except Exception as e:
            error_msg = f"Failed to load data to MySQL: {e}"
            self.logger.error(error_msg)
            raise
    
    def test_connection(self):
        """
        Test the MySQL connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        self.logger.info("Testing MySQL connection...")
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
        logger = get_logger('mysql_loader_test', log_config)
        
        print("\n" + "="*80)
        print("MYSQL LOADER TEST")
        print("="*80)
        
        # Check if MySQL is enabled
        if not config_loader.get('destinations.mysql_backup.enabled'):
            print("\n⚠️  MySQL destination is DISABLED in configuration")
            print("\nTo enable MySQL loader:")
            print("1. Install MySQL database")
            print("2. Set 'enabled: true' in config/pipeline_config.yaml")
            print("3. Update database credentials")
            print("\n✓ Loader code is ready - just enable when you have a database!")
        else:
            mysql_config = config_loader.get_destination_config('mysql_backup')
            loader = MySQLLoader(mysql_config, logger)
            
            if loader.test_connection():
                print("\n✓ MySQL connection successful!")
                print("✓ Ready to load data")
            else:
                print("\n✗ MySQL connection failed")
                print("Check your database credentials in config/pipeline_config.yaml")
        
        print("\n" + "="*80 + "\n")
        
    except Exception as e:
        print(f"\nNote: {e}")
        print("\nThis is normal if you don't have MySQL installed.")
        print("The loader is ready to use when you set up a database!")
        print("\n" + "="*80 + "\n")