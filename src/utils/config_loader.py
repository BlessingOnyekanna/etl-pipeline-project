"""
Configuration Loader Module
============================
This module handles loading and validating the YAML configuration file.
It provides a single source of truth for all pipeline settings.

Business Logic:
- Configuration should be external to code for easy customization
- Validate config on load to catch errors early
- Provide sensible defaults for optional settings
- Make it easy to switch between environments (dev, staging, prod)
"""

import yaml
import os
from typing import Dict, Any


class ConfigLoader:
    """
    Loads and validates pipeline configuration from YAML file.
    
    This class ensures configuration is properly structured and contains
    all required settings before the pipeline starts executing.
    """
    
    def __init__(self, config_path='config/pipeline_config.yaml'):
        """
        Initialize the configuration loader.
        
        Args:
            config_path (str): Path to YAML configuration file
        """
        self.config_path = config_path
        self.config = None
        self.load_config()
    
    def load_config(self):
        """
        Load configuration from YAML file.
        
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please ensure the config file exists at this location."
            )
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            # Validate configuration structure
            self._validate_config()
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
    
    def _validate_config(self):
        """
        Validate that configuration contains required sections.
        
        Raises:
            ValueError: If required configuration sections are missing
        """
        required_sections = ['pipeline', 'sources', 'transform', 'destinations', 'logging']
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(
                    f"Required configuration section '{section}' is missing.\n"
                    f"Please check your config file: {self.config_path}"
                )
        
        # Validate at least one source is enabled
        sources = self.config.get('sources', {})
        enabled_sources = [
            source for source, config in sources.items()
            if isinstance(config, dict) and config.get('enabled', False)
        ]
        
        if not enabled_sources:
            raise ValueError(
                "At least one data source must be enabled in configuration.\n"
                "Set 'enabled: true' for at least one source in the 'sources' section."
            )
        
        # Validate at least one destination is enabled
        destinations = self.config.get('destinations', {})
        enabled_destinations = [
            dest for dest, config in destinations.items()
            if isinstance(config, dict) and config.get('enabled', False)
        ]
        
        if not enabled_destinations:
            raise ValueError(
                "At least one destination must be enabled in configuration.\n"
                "Set 'enabled: true' for at least one destination in the 'destinations' section."
            )
    
    def get(self, key_path, default=None):
        """
        Get a configuration value using dot notation.
        
        Args:
            key_path (str): Dot-separated path to config value (e.g., 'sources.csv.enabled')
            default: Default value if key doesn't exist
            
        Returns:
            Any: Configuration value or default
            
        Example:
            >>> config.get('sources.csv.file_path')
            'data/raw/shipping_data.csv'
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_enabled_sources(self):
        """
        Get list of enabled data sources.
        
        Returns:
            list: Names of enabled sources
        """
        sources = self.config.get('sources', {})
        return [
            source for source, config in sources.items()
            if isinstance(config, dict) and config.get('enabled', False)
        ]
    
    def get_enabled_destinations(self):
        """
        Get list of enabled destinations.
        
        Returns:
            list: Names of enabled destinations
        """
        destinations = self.config.get('destinations', {})
        return [
            dest for dest, config in destinations.items()
            if isinstance(config, dict) and config.get('enabled', False)
        ]
    
    def get_source_config(self, source_name):
        """
        Get configuration for a specific data source.
        
        Args:
            source_name (str): Name of the source (csv, api, mysql, postgresql)
            
        Returns:
            dict: Source configuration
            
        Raises:
            ValueError: If source doesn't exist or is not enabled
        """
        source_config = self.config.get('sources', {}).get(source_name)
        
        if not source_config:
            raise ValueError(f"Source '{source_name}' not found in configuration")
        
        if not source_config.get('enabled', False):
            raise ValueError(f"Source '{source_name}' is not enabled in configuration")
        
        return source_config
    
    def get_destination_config(self, destination_name):
        """
        Get configuration for a specific destination.
        
        Args:
            destination_name (str): Name of the destination
            
        Returns:
            dict: Destination configuration
            
        Raises:
            ValueError: If destination doesn't exist or is not enabled
        """
        dest_config = self.config.get('destinations', {}).get(destination_name)
        
        if not dest_config:
            raise ValueError(f"Destination '{destination_name}' not found in configuration")
        
        if not dest_config.get('enabled', False):
            raise ValueError(f"Destination '{destination_name}' is not enabled")
        
        return dest_config
    
    def get_transform_config(self):
        """
        Get transformation configuration.
        
        Returns:
            dict: Transform configuration
        """
        return self.config.get('transform', {})
    
    def get_logging_config(self):
        """
        Get logging configuration.
        
        Returns:
            dict: Logging configuration
        """
        return self.config.get('logging', {})
    
    def get_error_handling_config(self):
        """
        Get error handling configuration.
        
        Returns:
            dict: Error handling configuration
        """
        return self.config.get('error_handling', {})
    
    def get_pipeline_info(self):
        """
        Get pipeline metadata.
        
        Returns:
            dict: Pipeline information (name, version, description, environment)
        """
        return self.config.get('pipeline', {})
    
    def display_config_summary(self):
        """
        Display a summary of the loaded configuration.
        Useful for debugging and verification.
        """
        print("\n" + "=" * 80)
        print("PIPELINE CONFIGURATION SUMMARY")
        print("=" * 80)
        
        # Pipeline info
        pipeline_info = self.get_pipeline_info()
        print(f"\nPipeline: {pipeline_info.get('name', 'N/A')}")
        print(f"Version: {pipeline_info.get('version', 'N/A')}")
        print(f"Environment: {pipeline_info.get('environment', 'N/A')}")
        
        # Enabled sources
        print(f"\nEnabled Sources: {', '.join(self.get_enabled_sources())}")
        
        # Enabled destinations
        print(f"Enabled Destinations: {', '.join(self.get_enabled_destinations())}")
        
        # Logging
        log_config = self.get_logging_config()
        print(f"\nLog Level: {log_config.get('level', 'N/A')}")
        print(f"Log File: {log_config.get('log_file', 'N/A')}")
        
        print("=" * 80 + "\n")


# Example usage and testing
if __name__ == "__main__":
    # Test configuration loading
    try:
        config = ConfigLoader('config/pipeline_config.yaml')
        config.display_config_summary()
        
        # Test getting specific values
        print("\nTesting configuration access:")
        print(f"CSV file path: {config.get('sources.csv.file_path')}")
        print(f"API base URL: {config.get('sources.api.base_url')}")
        print(f"Log level: {config.get('logging.level')}")
        
    except Exception as e:
        print(f"Error loading configuration: {e}")