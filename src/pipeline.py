"""
ETL Pipeline Orchestrator
=========================
Main pipeline that coordinates Extract, Transform, and Load operations.

Business Logic:
- ETL pipelines should be automated end-to-end
- Each step (Extract, Transform, Load) should be independent and testable
- Centralized orchestration makes it easy to run the complete pipeline
- Error handling ensures pipeline doesn't fail silently
"""

import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils import ConfigLoader, get_logger, PipelineLogger
from src.extractors.csv_extractor import CSVExtractor
from src.extractors.api_extractor import APIExtractor
from src.transformers.data_cleaner import DataCleaner
from src.transformers.data_merger import DataMerger
from src.transformers.data_validator import DataValidator
from src.loaders.csv_loader import CSVLoader
from src.loaders.cloud_loader import CloudLoader


class ETLPipeline:
    """
    Main ETL Pipeline orchestrator.
    
    This class coordinates the complete data pipeline:
    1. Extract data from multiple sources
    2. Transform (clean, validate, standardize)
    3. Load to multiple destinations
    """
    
    def __init__(self, config_path='config/pipeline_config.yaml'):
        """
        Initialize the ETL pipeline.
        
        Args:
            config_path (str): Path to configuration file
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        
        # Set up logging
        log_config = self.config_loader.get_logging_config()
        self.logger = get_logger('etl_pipeline', log_config)
        self.pipeline_logger = PipelineLogger('etl_pipeline', log_config)
        
        # Get pipeline info
        self.pipeline_info = self.config_loader.get_pipeline_info()
        
        # Storage for extracted data
        self.extracted_data = {}
        self.transformed_data = None
        
        self.logger.info("ETL Pipeline initialized")
    
    def run(self):
        """
        Run the complete ETL pipeline.
        
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        pipeline_name = self.pipeline_info.get('name', 'ETL Pipeline')
        
        try:
            # Log pipeline start
            self.pipeline_logger.log_pipeline_start(pipeline_name)
            
            # Execute pipeline steps
            self.extract()
            self.transform()
            self.load()
            
            # Log pipeline success
            self.pipeline_logger.log_pipeline_end(pipeline_name, success=True)
            self.logger.info("Pipeline completed successfully!")
            
            return True
            
        except Exception as e:
            # Log pipeline failure
            self.pipeline_logger.log_pipeline_end(pipeline_name, success=False)
            self.logger.error(f"Pipeline failed: {e}")
            
            import traceback
            traceback.print_exc()
            
            return False
    
    def extract(self):
        """
        Extract data from all enabled sources.
        
        Returns:
            dict: Dictionary of extracted DataFrames
        """
        self.pipeline_logger.log_step_start("EXTRACT")
        
        enabled_sources = self.config_loader.get_enabled_sources()
        self.logger.info(f"Extracting from {len(enabled_sources)} sources: {enabled_sources}")
        
        total_records = 0
        
        for source_name in enabled_sources:
            try:
                source_config = self.config_loader.get_source_config(source_name)
                
                if source_name == 'csv':
                    extractor = CSVExtractor(source_config, self.logger)
                    df = extractor.extract()
                    self.extracted_data['csv'] = df
                    total_records += len(df)
                    
                elif source_name == 'api':
                    extractor = APIExtractor(source_config, self.logger)
                    results = extractor.extract()
                    
                    # API returns dict of DataFrames
                    for endpoint, df in results.items():
                        if df is not None:
                            key = f'api_{endpoint}'
                            self.extracted_data[key] = df
                            total_records += len(df)
                
                # Note: MySQL and PostgreSQL extractors would go here
                # They're disabled by default until databases are set up
                
            except Exception as e:
                self.logger.error(f"Failed to extract from '{source_name}': {e}")
                # Continue with other sources
        
        self.pipeline_logger.log_step_end("EXTRACT", total_records)
        
        return self.extracted_data
    
    def transform(self):
        """
        Transform (clean, validate, standardize) extracted data.
        
        Returns:
            pd.DataFrame: Transformed data
        """
        self.pipeline_logger.log_step_start("TRANSFORM")
        
        if not self.extracted_data:
            raise ValueError("No data to transform. Run extract() first.")
        
        # Get transform configuration
        transform_config = self.config_loader.get_transform_config()
        
        # Initialize transformers
        cleaner = DataCleaner(transform_config, self.logger)
        merger = DataMerger(transform_config, self.logger)
        validator = DataValidator(transform_config, self.logger)
        
        # Clean each source
        cleaned_data = {}
        for source_name, df in self.extracted_data.items():
            self.logger.info(f"Cleaning data from '{source_name}'")
            df_clean = cleaner.clean(df, source_name)
            cleaned_data[source_name] = df_clean
        
        # Merge all sources
        self.logger.info("Merging data from all sources")
        merged_data = merger.merge(cleaned_data)
        
        # Standardize merged data
        self.logger.info("Standardizing data formats")
        self.transformed_data = merger.standardize(merged_data)
        
        # Validate final data
        self.logger.info("Validating data quality")
        validation_results = validator.validate(
            self.transformed_data,
            source_name="pipeline_output"
        )
        
        quality_score = validation_results['quality_score']
        self.logger.info(f"Data quality score: {quality_score}/100")
        
        self.pipeline_logger.log_step_end("TRANSFORM", len(self.transformed_data))
        
        return self.transformed_data
    
    def load(self):
        """
        Load transformed data to all enabled destinations.
        
        Returns:
            dict: Load results for each destination
        """
        self.pipeline_logger.log_step_start("LOAD")
        
        if self.transformed_data is None:
            raise ValueError("No data to load. Run transform() first.")
        
        enabled_destinations = self.config_loader.get_enabled_destinations()
        self.logger.info(f"Loading to {len(enabled_destinations)} destinations: {enabled_destinations}")
        
        load_results = {}
        total_loaded = 0
        
        for dest_name in enabled_destinations:
            try:
                dest_config = self.config_loader.get_destination_config(dest_name)
                
                if dest_name == 'csv_export':
                    loader = CSVLoader(dest_config, self.logger)
                    output_path = loader.load(self.transformed_data)
                    load_results[dest_name] = {'status': 'success', 'path': output_path}
                    total_loaded += len(self.transformed_data)
                    
                elif dest_name == 'cloud_storage':
                    loader = CloudLoader(dest_config, self.logger)
                    # Load as both CSV and JSON
                    csv_path = loader.load(
                        self.transformed_data,
                        dataset_name="ecommerce_analytics",
                        file_format="csv"
                    )
                    json_path = loader.load(
                        self.transformed_data,
                        dataset_name="ecommerce_analytics",
                        file_format="json"
                    )
                    load_results[dest_name] = {
                        'status': 'success',
                        'csv_path': csv_path,
                        'json_path': json_path
                    }
                    total_loaded += len(self.transformed_data) * 2  # Count both formats
                
                # Note: PostgreSQL and MySQL loaders would go here
                # They're disabled by default until databases are set up
                
            except Exception as e:
                self.logger.error(f"Failed to load to '{dest_name}': {e}")
                load_results[dest_name] = {'status': 'failed', 'error': str(e)}
        
        self.pipeline_logger.log_step_end("LOAD", total_loaded)
        
        return load_results


def main():
    """
    Main entry point for running the pipeline.
    """
    print("\n" + "="*80)
    print("ETL PIPELINE - EXECUTION")
    print("="*80 + "\n")
    
    # Create and run pipeline
    pipeline = ETLPipeline()
    
    # Display configuration summary
    pipeline.config_loader.display_config_summary()
    
    # Run pipeline
    success = pipeline.run()
    
    # Display results
    print("\n" + "="*80)
    if success:
        print("✓ PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"\nProcessed Records: {len(pipeline.transformed_data)}")
        print(f"Output Locations:")
        print(f"  - CSV: data/processed/sales_analytics.csv")
        print(f"  - Cloud: data/cloud_storage/ecommerce_analytics/")
        print(f"  - Quality Report: reports/data_quality_report.txt")
    else:
        print("✗ PIPELINE FAILED")
        print("="*80)
        print("\nCheck logs/pipeline.log for details")
    
    print("="*80 + "\n")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())