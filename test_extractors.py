"""
Test Extractors Script
======================
Simple script to test all extractors from the project root.
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils import ConfigLoader, get_logger
from src.extractors.csv_extractor import CSVExtractor
from src.extractors.api_extractor import APIExtractor


def test_csv_extractor():
    """Test CSV extractor with your e-commerce data."""
    print("\n" + "="*80)
    print("TESTING CSV EXTRACTOR")
    print("="*80)
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        csv_config = config_loader.get_source_config('csv')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('csv_test', log_config)
        
        # Create extractor
        extractor = CSVExtractor(csv_config, logger)
        
        # Validate and extract
        if extractor.validate_file():
            df = extractor.extract()
            
            print("\n✅ CSV EXTRACTION SUCCESSFUL!")
            print(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"\nColumns: {list(df.columns)}")
            print(f"\nFirst 5 rows:")
            print(df.head())
            print("\n" + "="*80)
            
            return df
        else:
            print("❌ File validation failed!")
            return None
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_api_extractor():
    """Test API extractor with Fake Store API."""
    print("\n" + "="*80)
    print("TESTING API EXTRACTOR")
    print("="*80)
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        api_config = config_loader.get_source_config('api')
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('api_test', log_config)
        
        # Create extractor
        extractor = APIExtractor(api_config, logger)
        
        # Extract from all endpoints
        results = extractor.extract()
        
        print("\n✅ API EXTRACTION SUCCESSFUL!")
        
        for endpoint_name, df in results.items():
            if df is not None:
                print(f"\n{endpoint_name.upper()}:")
                print(f"  Shape: {df.shape[0]} rows, {df.shape[1]} columns")
                print(f"  Columns: {list(df.columns)[:5]}...")  # Show first 5 columns
                print(f"  First 2 rows:")
                print(df.head(2))
            else:
                print(f"\n{endpoint_name.upper()}: ❌ FAILED")
        
        print("\n" + "="*80)
        return results
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Run all extractor tests."""
    print("\n" + "="*80)
    print("ETL PIPELINE - EXTRACTOR TESTS")
    print("="*80)
    
    # Test CSV extractor
    csv_data = test_csv_extractor()
    
    # Test API extractor
    api_data = test_api_extractor()
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"CSV Extractor: {'✅ PASS' if csv_data is not None else '❌ FAIL'}")
    print(f"API Extractor: {'✅ PASS' if api_data is not None else '❌ FAIL'}")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()