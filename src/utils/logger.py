"""
Logging Utility Module
======================
This module provides a centralized logging system for the ETL pipeline.
It creates both file and console loggers with proper formatting.

Business Logic:
- All pipeline activities should be logged for auditing and debugging
- Separate log levels help filter information (DEBUG for development, INFO for production)
- File logs persist for historical analysis
- Console logs provide real-time feedback during execution
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime


class PipelineLogger:
    """
    Centralized logger for ETL pipeline operations.
    
    This class creates a logger that writes to both file and console,
    making it easy to track pipeline execution and troubleshoot issues.
    """
    
    def __init__(self, name, log_config):
        """
        Initialize the pipeline logger.
        
        Args:
            name (str): Name of the logger (usually module name)
            log_config (dict): Logging configuration from YAML file
        """
        self.name = name
        self.config = log_config
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """
        Set up the logger with file and console handlers.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        # Create logger
        logger = logging.getLogger(self.name)
        logger.setLevel(self._get_log_level())
        
        # Prevent duplicate handlers if logger already exists
        if logger.handlers:
            return logger
        
        # Create formatters
        formatter = logging.Formatter(
            self.config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console Handler (logs to terminal)
        if self.config.get('log_to_console', True):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self._get_log_level())
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File Handler (logs to file with rotation)
        if self.config.get('log_to_file', True):
            log_file = self.config.get('log_file', 'logs/pipeline.log')
            
            # Create logs directory if it doesn't exist
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            # Rotating file handler prevents log files from becoming too large
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=self.config.get('max_file_size_mb', 10) * 1024 * 1024,  # Convert MB to bytes
                backupCount=self.config.get('backup_count', 5)
            )
            file_handler.setLevel(self._get_log_level())
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    def _get_log_level(self):
        """
        Convert string log level to logging constant.
        
        Returns:
            int: Logging level constant
        """
        level_str = self.config.get('level', 'INFO').upper()
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        return level_map.get(level_str, logging.INFO)
    
    def get_logger(self):
        """
        Get the configured logger instance.
        
        Returns:
            logging.Logger: Logger instance
        """
        return self.logger
    
    def log_pipeline_start(self, pipeline_name):
        """
        Log the start of a pipeline run.
        
        Args:
            pipeline_name (str): Name of the pipeline
        """
        self.logger.info("=" * 80)
        self.logger.info(f"PIPELINE START: {pipeline_name}")
        self.logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)
    
    def log_pipeline_end(self, pipeline_name, success=True):
        """
        Log the end of a pipeline run.
        
        Args:
            pipeline_name (str): Name of the pipeline
            success (bool): Whether pipeline completed successfully
        """
        status = "SUCCESS" if success else "FAILED"
        self.logger.info("=" * 80)
        self.logger.info(f"PIPELINE END: {pipeline_name} - {status}")
        self.logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)
    
    def log_step_start(self, step_name):
        """
        Log the start of a pipeline step (Extract, Transform, Load).
        
        Args:
            step_name (str): Name of the step
        """
        self.logger.info("-" * 80)
        self.logger.info(f"STEP START: {step_name}")
        self.logger.info("-" * 80)
    
    def log_step_end(self, step_name, records_processed=None):
        """
        Log the end of a pipeline step.
        
        Args:
            step_name (str): Name of the step
            records_processed (int, optional): Number of records processed
        """
        msg = f"STEP END: {step_name}"
        if records_processed is not None:
            msg += f" - {records_processed} records processed"
        self.logger.info(msg)
        self.logger.info("-" * 80)


def get_logger(name, config):
    """
    Factory function to get a pipeline logger.
    
    Args:
        name (str): Logger name
        config (dict): Logging configuration
        
    Returns:
        logging.Logger: Configured logger
    """
    pipeline_logger = PipelineLogger(name, config)
    return pipeline_logger.get_logger()


# Example usage:
if __name__ == "__main__":
    # Test logging setup
    test_config = {
        'level': 'DEBUG',
        'log_to_file': True,
        'log_file': 'logs/test.log',
        'log_to_console': True,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'max_file_size_mb': 5,
        'backup_count': 3
    }
    
    logger = get_logger('test_logger', test_config)
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")