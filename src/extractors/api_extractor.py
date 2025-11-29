"""
API Extractor Module
====================
Extracts data from REST APIs into pandas DataFrames.

Business Logic:
- APIs are common data sources (Shopify, Stripe, internal company APIs)
- Network requests can fail, so we need retry logic
- Rate limiting is common - don't overwhelm the API
- JSON responses need to be converted to DataFrames
"""

import requests
import pandas as pd
import time
from typing import Optional, Dict, Any, List


class APIExtractor:
    """
    Extracts data from REST APIs.
    
    This extractor handles API calls with retry logic, timeout handling,
    and automatic JSON to DataFrame conversion.
    """
    
    def __init__(self, config, logger):
        """
        Initialize API extractor.
        
        Args:
            config (dict): API source configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.base_url = config.get('base_url')
        self.endpoints = config.get('endpoints', {})
        self.timeout = config.get('timeout', 30)
        self.retry_attempts = config.get('retry_attempts', 3)
        self.headers = config.get('headers', {})
    
    def extract(self, endpoint_name=None):
        """
        Extract data from API endpoint(s).
        
        Args:
            endpoint_name (str, optional): Specific endpoint to call.
                                          If None, calls all configured endpoints.
        
        Returns:
            pd.DataFrame or dict: Extracted data
        """
        if endpoint_name:
            # Extract from single endpoint
            return self._extract_single_endpoint(endpoint_name)
        else:
            # Extract from all endpoints
            return self._extract_all_endpoints()
    
    def _extract_single_endpoint(self, endpoint_name):
        """
        Extract data from a single API endpoint.
        
        Args:
            endpoint_name (str): Name of the endpoint (e.g., 'products', 'users')
            
        Returns:
            pd.DataFrame: Extracted data
        """
        if endpoint_name not in self.endpoints:
            raise ValueError(
                f"Endpoint '{endpoint_name}' not found in configuration. "
                f"Available endpoints: {list(self.endpoints.keys())}"
            )
        
        endpoint_path = self.endpoints[endpoint_name]
        url = f"{self.base_url}{endpoint_path}"
        
        self.logger.info(f"Extracting data from API: {url}")
        
        # Make API call with retry logic
        data = self._make_request_with_retry(url)
        
        # Convert JSON to DataFrame
        df = self._json_to_dataframe(data, endpoint_name)
        
        return df
    
    def _extract_all_endpoints(self):
        """
        Extract data from all configured endpoints.
        
        Returns:
            dict: Dictionary of DataFrames, keyed by endpoint name
        """
        self.logger.info(f"Extracting data from {len(self.endpoints)} API endpoints")
        
        results = {}
        
        for endpoint_name in self.endpoints:
            try:
                df = self._extract_single_endpoint(endpoint_name)
                results[endpoint_name] = df
                self.logger.info(f"Successfully extracted '{endpoint_name}': {len(df)} records")
            except Exception as e:
                self.logger.error(f"Failed to extract '{endpoint_name}': {e}")
                # Continue with other endpoints even if one fails
                results[endpoint_name] = None
        
        return results
    
    def _make_request_with_retry(self, url, method='GET', max_retries=None):
        """
        Make HTTP request with retry logic.
        
        Args:
            url (str): API URL
            method (str): HTTP method (GET, POST, etc.)
            max_retries (int): Maximum number of retry attempts
            
        Returns:
            dict or list: JSON response data
        """
        if max_retries is None:
            max_retries = self.retry_attempts
        
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"API request attempt {attempt}/{max_retries}: {url}")
                
                # Make the request
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    timeout=self.timeout
                )
                
                # Check if request was successful
                response.raise_for_status()
                
                # Parse JSON response
                data = response.json()
                
                self.logger.info(f"API request successful: {url}")
                return data
                
            except requests.exceptions.Timeout:
                last_error = f"Request timed out after {self.timeout} seconds"
                self.logger.warning(f"Attempt {attempt} - {last_error}")
                
            except requests.exceptions.HTTPError as e:
                last_error = f"HTTP error: {e}"
                self.logger.warning(f"Attempt {attempt} - {last_error}")
                
            except requests.exceptions.RequestException as e:
                last_error = f"Request error: {e}"
                self.logger.warning(f"Attempt {attempt} - {last_error}")
            
            # Wait before retrying (exponential backoff)
            if attempt < max_retries:
                wait_time = 2 ** attempt  # 2, 4, 8 seconds
                self.logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        # All retries failed
        error_msg = f"API request failed after {max_retries} attempts. Last error: {last_error}"
        self.logger.error(error_msg)
        raise Exception(error_msg)
    
    def _json_to_dataframe(self, data, endpoint_name):
        """
        Convert JSON response to pandas DataFrame.
        
        Args:
            data: JSON data (dict or list)
            endpoint_name (str): Name of the endpoint (for logging)
            
        Returns:
            pd.DataFrame: Converted data
        """
        try:
            if isinstance(data, list):
                # Data is already a list of records
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Data might be wrapped in a key
                # Try to find the actual data array
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                elif 'results' in data:
                    df = pd.DataFrame(data['results'])
                elif 'items' in data:
                    df = pd.DataFrame(data['items'])
                else:
                    # Convert dict to single-row DataFrame
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unexpected data type: {type(data)}")
            
            rows, cols = df.shape
            self.logger.info(
                f"Converted '{endpoint_name}' to DataFrame: {rows} rows, {cols} columns"
            )
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to convert JSON to DataFrame: {e}"
            self.logger.error(error_msg)
            raise


# Example usage and testing
if __name__ == "__main__":
    from src.utils import ConfigLoader, get_logger
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        api_config = config_loader.get_source_config('api')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('api_extractor_test', log_config)
        
        # Create extractor
        extractor = APIExtractor(api_config, logger)
        
        # Extract from all endpoints
        print("\n" + "="*80)
        print("API EXTRACTION TEST")
        print("="*80)
        
        results = extractor.extract()
        
        for endpoint_name, df in results.items():
            if df is not None:
                print(f"\n{endpoint_name.upper()}:")
                print(f"  Shape: {df.shape}")
                print(f"  Columns: {list(df.columns)}")
                print(f"  First 3 rows:")
                print(df.head(3))
            else:
                print(f"\n{endpoint_name.upper()}: FAILED")
        
        print("="*80)
        
    except Exception as e:
        print(f"Test failed: {e}")