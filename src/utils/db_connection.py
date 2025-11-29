"""
Database Connection Utilities
==============================
This module provides reusable database connection management for MySQL and PostgreSQL.
It includes connection pooling, error handling, and helper functions.

Business Logic:
- Database connections should be reusable and properly closed
- Connection errors should be caught and logged
- Provide simple interface for common database operations
- Support both reading and writing data
"""

import pandas as pd
from typing import Optional, Dict, Any, List


class DatabaseConnection:
    """
    Base class for database connections.
    Provides common functionality for both MySQL and PostgreSQL.
    """
    
    def __init__(self, config, logger):
        """
        Initialize database connection.
        
        Args:
            config (dict): Database configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        self.connection = None
    
    def connect(self):
        """Connect to database. Implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement connect()")
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info(f"Database connection closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing database connection: {e}")
    
    def test_connection(self):
        """
        Test database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connect()
            self.logger.info("Database connection test successful")
            self.disconnect()
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False


class PostgreSQLConnection(DatabaseConnection):
    """
    PostgreSQL database connection manager.
    Handles connections to PostgreSQL databases.
    """
    
    def connect(self):
        """
        Establish connection to PostgreSQL database.
        
        Returns:
            connection: Database connection
        """
        try:
            import psycopg2
            
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.logger.info(f"Connected to PostgreSQL: {self.config['database']}")
            return self.connection
            
        except ImportError:
            self.logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
            raise
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
    
    def read_table(self, table_name, schema='public', query=None):
        """
        Read data from PostgreSQL table into DataFrame.
        
        Args:
            table_name (str): Name of the table to read
            schema (str): Database schema (default: 'public')
            query (str, optional): Custom SQL query. If None, reads entire table.
            
        Returns:
            pd.DataFrame: Data from table
        """
        try:
            self.connect()
            
            if query:
                sql = query
                self.logger.info(f"Executing custom query on PostgreSQL")
            else:
                sql = f'SELECT * FROM {schema}.{table_name}'
                self.logger.info(f"Reading table: {schema}.{table_name}")
            
            df = pd.read_sql(sql, self.connection)
            self.logger.info(f"Successfully read {len(df)} rows from PostgreSQL")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading from PostgreSQL: {e}")
            raise
        finally:
            self.disconnect()
    
    def write_table(self, df, table_name, schema='public', if_exists='replace'):
        """
        Write DataFrame to PostgreSQL table.
        
        Args:
            df (pd.DataFrame): Data to write
            table_name (str): Name of the table
            schema (str): Database schema (default: 'public')
            if_exists (str): What to do if table exists ('fail', 'replace', 'append')
        """
        try:
            self.connect()
            
            # Create schema if it doesn't exist
            with self.connection.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                self.connection.commit()
            
            # Write DataFrame to table
            df.to_sql(
                name=table_name,
                con=self.connection,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            
            self.logger.info(
                f"Successfully wrote {len(df)} rows to {schema}.{table_name} "
                f"(mode: {if_exists})"
            )
            
        except Exception as e:
            self.logger.error(f"Error writing to PostgreSQL: {e}")
            raise
        finally:
            self.disconnect()


class MySQLConnection(DatabaseConnection):
    """
    MySQL database connection manager.
    Handles connections to MySQL databases.
    """
    
    def connect(self):
        """
        Establish connection to MySQL database.
        
        Returns:
            connection: Database connection
        """
        try:
            import mysql.connector
            
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.logger.info(f"Connected to MySQL: {self.config['database']}")
            return self.connection
            
        except ImportError:
            self.logger.error("mysql-connector-python not installed. Run: pip install mysql-connector-python")
            raise
        except Exception as e:
            self.logger.error(f"MySQL connection error: {e}")
            raise
    
    def read_table(self, table_name, query=None):
        """
        Read data from MySQL table into DataFrame.
        
        Args:
            table_name (str): Name of the table to read
            query (str, optional): Custom SQL query. If None, reads entire table.
            
        Returns:
            pd.DataFrame: Data from table
        """
        try:
            self.connect()
            
            if query:
                sql = query
                self.logger.info(f"Executing custom query on MySQL")
            else:
                sql = f'SELECT * FROM {table_name}'
                self.logger.info(f"Reading table: {table_name}")
            
            df = pd.read_sql(sql, self.connection)
            self.logger.info(f"Successfully read {len(df)} rows from MySQL")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading from MySQL: {e}")
            raise
        finally:
            self.disconnect()
    
    def write_table(self, df, table_name, if_exists='replace'):
        """
        Write DataFrame to MySQL table.
        
        Args:
            df (pd.DataFrame): Data to write
            table_name (str): Name of the table
            if_exists (str): What to do if table exists ('fail', 'replace', 'append')
        """
        try:
            self.connect()
            
            # Write DataFrame to table
            df.to_sql(
                name=table_name,
                con=self.connection,
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            
            self.logger.info(
                f"Successfully wrote {len(df)} rows to {table_name} "
                f"(mode: {if_exists})"
            )
            
        except Exception as e:
            self.logger.error(f"Error writing to MySQL: {e}")
            raise
        finally:
            self.disconnect()


def get_database_connection(db_type, config, logger):
    """
    Factory function to get appropriate database connection.
    
    Args:
        db_type (str): Type of database ('postgresql' or 'mysql')
        config (dict): Database configuration
        logger: Logger instance
        
    Returns:
        DatabaseConnection: Database connection instance
        
    Raises:
        ValueError: If db_type is not supported
    """
    if db_type.lower() == 'postgresql':
        return PostgreSQLConnection(config, logger)
    elif db_type.lower() == 'mysql':
        return MySQLConnection(config, logger)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")