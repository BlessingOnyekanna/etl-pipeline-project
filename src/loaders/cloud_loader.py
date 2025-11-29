"""
Cloud Storage Loader Module
============================
Simulates loading data to cloud storage (S3, Azure Blob, Google Cloud Storage).

Business Logic:
- Cloud storage is used for data lakes and archives
- Partition data by date for efficient querying
- Multiple file formats supported
- Simulated locally for development/testing
"""

import pandas as pd
import os
from datetime import datetime


class CloudLoader:
    """
    Simulates loading data to cloud storage.
    
    This loader creates a local folder structure that mimics
    cloud storage organization with partitioning.
    """
    
    def __init__(self, config, logger):
        """
        Initialize cloud storage loader.
        
        Args:
            config (dict): Cloud storage configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.base_path = config.get('base_path', 'data/cloud_storage')
        self.partition_by = config.get('partition_by', 'date')
    
    def load(self, df, dataset_name="data", file_format="csv"):
        """
        Load DataFrame to cloud storage (simulated).
        
        Business Logic:
        - Partition by date: year=2024/month=11/day=29/
        - Each partition contains one file
        - Mimics AWS S3 / Azure Blob structure
        
        Args:
            df (pd.DataFrame): Data to upload
            dataset_name (str): Name of the dataset
            file_format (str): File format ('csv', 'parquet', 'json')
        
        Returns:
            str: Path to uploaded file
        """
        self.logger.info(f"Uploading data to cloud storage: {dataset_name}")
        
        # Create partition path
        if self.partition_by == 'date':
            now = datetime.now()
            partition_path = os.path.join(
                self.base_path,
                dataset_name,
                f"year={now.year}",
                f"month={now.month:02d}",
                f"day={now.day:02d}"
            )
        else:
            partition_path = os.path.join(self.base_path, dataset_name)
        
        # Create directory
        os.makedirs(partition_path, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{dataset_name}_{timestamp}.{file_format}"
        file_path = os.path.join(partition_path, filename)
        
        try:
            # Save based on format
            if file_format == 'csv':
                df.to_csv(file_path, index=False)
            elif file_format == 'parquet':
                df.to_parquet(file_path, index=False)
            elif file_format == 'json':
                df.to_json(file_path, orient='records', lines=True)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Log success
            rows, cols = df.shape
            file_size = os.path.getsize(file_path) / 1024  # KB
            
            self.logger.info(
                f"Cloud upload successful: {rows} rows, {cols} columns "
                f"({file_size:.2f} KB) -> {file_path}"
            )
            
            # Simulate cloud URL
            cloud_url = f"s3://my-bucket/{file_path.replace(os.sep, '/')}"
            self.logger.info(f"Simulated cloud URL: {cloud_url}")
            
            return file_path
            
        except Exception as e:
            error_msg = f"Failed to upload to cloud storage: {e}"
            self.logger.error(error_msg)
            raise
    
    def list_partitions(self, dataset_name):
        """
        List all partitions for a dataset.
        
        Args:
            dataset_name (str): Name of the dataset
            
        Returns:
            list: List of partition paths
        """
        dataset_path = os.path.join(self.base_path, dataset_name)
        
        if not os.path.exists(dataset_path):
            return []
        
        partitions = []
        for root, dirs, files in os.walk(dataset_path):
            if files:  # Directory contains files
                partitions.append(root)
        
        return partitions


# Example usage and testing
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from src.utils import ConfigLoader, get_logger
    from src.extractors.csv_extractor import CSVExtractor
    from src.transformers.data_cleaner import DataCleaner
    from src.transformers.data_merger import DataMerger
    
    try:
        # Load configuration
        config_loader = ConfigLoader('config/pipeline_config.yaml')
        cloud_config = config_loader.get_destination_config('cloud_storage')
        csv_config = config_loader.get_source_config('csv')
        transform_config = config_loader.get_transform_config()
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('cloud_loader_test', log_config)
        
        # Extract, clean, and standardize data
        print("\n" + "="*80)
        print("CLOUD STORAGE LOADER TEST")
        print("="*80)
        
        extractor = CSVExtractor(csv_config, logger)
        df = extractor.extract()
        
        cleaner = DataCleaner(transform_config, logger)
        df_clean = cleaner.clean(df, "e-commerce")
        
        merger = DataMerger(transform_config, logger)
        df_final = merger.standardize(df_clean)
        
        # Upload to cloud storage
        loader = CloudLoader(cloud_config, logger)
        
        # Upload as CSV
        csv_path = loader.load(df_final, dataset_name="ecommerce", file_format="csv")
        print(f"\n✓ Uploaded CSV: {csv_path}")
        
        # Upload as JSON
        json_path = loader.load(df_final, dataset_name="ecommerce", file_format="json")
        print(f"✓ Uploaded JSON: {json_path}")
        
        # List partitions
        partitions = loader.list_partitions("ecommerce")
        print(f"\n✓ Total partitions: {len(partitions)}")
        for partition in partitions:
            print(f"  - {partition}")
        
        print("\n" + "="*80)
        print("CLOUD STORAGE LOADER TEST COMPLETE")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()