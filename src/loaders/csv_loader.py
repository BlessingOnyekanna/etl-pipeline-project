"""
CSV Loader Module
=================
Loads data to CSV files for export and sharing.

Business Logic:
- CSV is universal format - works with Excel, databases, analytics tools
- Clients often need data exports for their own analysis
- CSV files can be shared via email, cloud storage, etc.
- Good for backups and data archiving
"""

import pandas as pd
import os
from datetime import datetime


class CSVLoader:
    """
    Loads data to CSV files.
    
    This loader exports DataFrames to CSV format with
    configurable options for compatibility.
    """
    
    def __init__(self, config, logger):
        """
        Initialize CSV loader.
        
        Args:
            config (dict): CSV destination configuration from YAML
            logger: Logger instance for tracking operations
        """
        self.config = config
        self.logger = logger
        self.output_path = config.get('output_path', 'data/processed/output.csv')
        self.include_index = config.get('include_index', False)
    
    def load(self, df, custom_path=None):
        """
        Load DataFrame to CSV file.
        
        Args:
            df (pd.DataFrame): Data to export
            custom_path (str, optional): Custom output path.
                                        Uses config default if None.
        
        Returns:
            str: Path to saved file
        """
        output_file = custom_path or self.output_path
        
        self.logger.info(f"Loading data to CSV: {output_file}")
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        try:
            # Export to CSV
            df.to_csv(
                output_file,
                index=self.include_index,
                encoding='utf-8'
            )
            
            # Log success
            rows, cols = df.shape
            file_size = os.path.getsize(output_file) / 1024  # KB
            
            self.logger.info(
                f"CSV export successful: {rows} rows, {cols} columns "
                f"({file_size:.2f} KB) -> {output_file}"
            )
            
            return output_file
            
        except Exception as e:
            error_msg = f"Failed to export CSV: {e}"
            self.logger.error(error_msg)
            raise
    
    def load_with_timestamp(self, df, prefix="export"):
        """
        Load DataFrame to CSV with timestamp in filename.
        
        Business Logic:
        - Timestamped files prevent overwrites
        - Good for daily/hourly exports
        - Creates audit trail
        
        Args:
            df (pd.DataFrame): Data to export
            prefix (str): Filename prefix
            
        Returns:
            str: Path to saved file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        directory = os.path.dirname(self.output_path)
        filename = f"{prefix}_{timestamp}.csv"
        timestamped_path = os.path.join(directory, filename)
        
        return self.load(df, custom_path=timestamped_path)


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
        csv_dest_config = config_loader.get_destination_config('csv_export')
        csv_src_config = config_loader.get_source_config('csv')
        transform_config = config_loader.get_transform_config()
        log_config = config_loader.get_logging_config()
        
        # Set up logger
        logger = get_logger('csv_loader_test', log_config)
        
        # Extract, clean, and standardize data
        print("\n" + "="*80)
        print("CSV LOADER TEST")
        print("="*80)
        
        extractor = CSVExtractor(csv_src_config, logger)
        df = extractor.extract()
        
        cleaner = DataCleaner(transform_config, logger)
        df_clean = cleaner.clean(df, "e-commerce")
        
        merger = DataMerger(transform_config, logger)
        df_final = merger.standardize(df_clean)
        
        # Load to CSV
        loader = CSVLoader(csv_dest_config, logger)
        
        # Regular export
        output_file = loader.load(df_final)
        print(f"\n✓ Exported to: {output_file}")
        
        # Timestamped export
        timestamped_file = loader.load_with_timestamp(df_final, prefix="ecommerce_clean")
        print(f"✓ Timestamped export: {timestamped_file}")
        
        print("\n" + "="*80)
        print("CSV LOADER TEST COMPLETE")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()