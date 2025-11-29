"""
Simple Standardization Test
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils import ConfigLoader, get_logger
from src.extractors.csv_extractor import CSVExtractor
from src.transformers.data_cleaner import DataCleaner
from src.transformers.data_merger import DataMerger
from src.transformers.data_validator import DataValidator

# Load configuration
config_loader = ConfigLoader('config/pipeline_config.yaml')
transform_config = config_loader.get_transform_config()
csv_config = config_loader.get_source_config('csv')
log_config = config_loader.get_logging_config()
logger = get_logger('test', log_config)

# Extract and clean
print("\n" + "="*80)
print("STEP 1: EXTRACT & CLEAN")
print("="*80)
extractor = CSVExtractor(csv_config, logger)
df = extractor.extract()

cleaner = DataCleaner(transform_config, logger)
df_clean = cleaner.clean(df, "e-commerce")

# Show BEFORE standardization
print("\n" + "="*80)
print("BEFORE STANDARDIZATION")
print("="*80)
print("\nFirst 5 rows:")
print(df_clean[['order_id', 'customer_name', 'status', 'quantity', 'price']].head())
print(f"\nNegative quantities: {(df_clean['quantity'] < 0).sum()}")
print(f"Price data type: {df_clean['price'].dtype}")

# Standardize
print("\n" + "="*80)
print("STEP 2: STANDARDIZE")
print("="*80)
merger = DataMerger(transform_config, logger)
df_std = merger.standardize(df_clean)

# Show AFTER standardization
print("\n" + "="*80)
print("AFTER STANDARDIZATION")
print("="*80)
print("\nFirst 5 rows:")
print(df_std[['order_id', 'customer_name', 'status', 'quantity', 'price']].head())
print(f"\nNegative quantities: {(df_std['quantity'] < 0).sum()}")
print(f"Price data type: {df_std['price'].dtype}")

# Validate
print("\n" + "="*80)
print("STEP 3: VALIDATE")
print("="*80)
validator = DataValidator(transform_config, logger)
results = validator.validate(df_std, "e-commerce_final")

print(f"\n✓ Quality Score: {results['quality_score']}/100")
print(f"✓ Completeness: {results['completeness']['completeness_percentage']}%")
print(f"✓ Uniqueness: {results['uniqueness']['uniqueness_percentage']}%")
print(f"✓ Validity: {results['validity']['validity_percentage']}%")
print(f"✓ Consistency: {results['consistency']['consistency_percentage']}%")

if results['validity']['issues']:
    print("\nValidity Issues:")
    for issue in results['validity']['issues']:
        print(f"  - {issue}")

if results['consistency']['issues']:
    print("\nConsistency Issues:")
    for issue in results['consistency']['issues']:
        print(f"  - {issue}")

print("\n✓ Report saved to: reports/data_quality_report.txt")
print("="*80 + "\n")