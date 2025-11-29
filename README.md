# 🔄 ETL Pipeline - Production-Ready Data Integration System

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive, modular ETL (Extract, Transform, Load) pipeline built for real-world data engineering scenarios. This project demonstrates production-ready data integration from multiple sources with robust error handling, logging, and configuration management.

## 🎯 Overview

This ETL pipeline simulates a real-world retail analytics scenario where data flows from multiple sources into a centralized analytics database.

**Use Case**: A retail company collecting daily sales data from their Shopify API, inventory data from MySQL, customer data from PostgreSQL, and shipping data from CSV files. The pipeline extracts, cleans, merges, and loads this data into an analytics database for reporting.

## ✨ Features

### Extract Component
- ✅ CSV file extraction with configurable delimiters
- ✅ REST API integration with retry logic
- ✅ MySQL database extraction
- ✅ PostgreSQL database extraction
- ✅ Modular, reusable extractors

### Transform Component
- ✅ Automated data cleaning (missing values, duplicates, outliers)
- ✅ Data type validation and conversion
- ✅ Multi-source data merging
- ✅ Data quality reporting
- ✅ Configurable transformation rules

### Load Component
- ✅ PostgreSQL loader with auto-table creation
- ✅ MySQL loader with schema management
- ✅ CSV export functionality
- ✅ Cloud storage simulation (local folders)
- ✅ Flexible load strategies (replace, append, fail)

### Pipeline Features
- ✅ YAML-based configuration (no code changes needed)
- ✅ Comprehensive logging system
- ✅ Error handling with retry logic
- ✅ Pipeline orchestration
- ✅ Performance monitoring
- ✅ Modular, maintainable architecture

## 📁 Project Structure
```
etl_pipeline_project/
│
├── config/
│   └── pipeline_config.yaml          # Main configuration file
│
├── src/
│   ├── extractors/                    # Data extraction modules
│   ├── transformers/                  # Data transformation modules
│   ├── loaders/                       # Data loading modules
│   ├── utils/                         # Utility modules
│   │   ├── logger.py                 # Logging system
│   │   ├── config_loader.py          # Configuration management
│   │   └── db_connection.py          # Database utilities
│   └── pipeline.py                    # Main pipeline orchestrator
│
├── data/
│   ├── raw/                           # Input data files
│   ├── processed/                     # Output data files
│   └── cloud_storage/                 # Simulated cloud storage
│
├── logs/                              # Pipeline execution logs
├── reports/                           # Data quality reports
└── requirements.txt                   # Python dependencies
```

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12+ (optional, for database sources/destinations)
- MySQL 8+ (optional, for database sources/destinations)

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/BlessingOnyekanna/etl_pipeline_project.git
cd etl_pipeline_project
```

2. **Create virtual environment**

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure the pipeline**
Edit `config/pipeline_config.yaml` with your settings:
- Database credentials
- File paths
- API endpoints
- Transformation rules

## ⚡ Quick Start

### Basic Pipeline Run
```bash
python src/pipeline.py
```

### Run with Custom Configuration
```bash
python src/pipeline.py --config config/my_pipeline_config.yaml
```

### Test Individual Components
```python
# Test configuration loading
python src/utils/config_loader.py

# Test logging
python src/utils/logger.py
```

## ⚙️ Configuration

The pipeline is controlled entirely through `config/pipeline_config.yaml`:
```yaml
# Example: Enable/disable data sources
sources:
  csv:
    enabled: true
    file_path: "data/raw/shipping_data.csv"
    
  api:
    enabled: true
    base_url: "https://fakestoreapi.com"

# Example: Configure transformations
transform:
  missing_values:
    numeric_strategy: "mean"  # mean, median, zero, drop
    categorical_strategy: "mode"  # mode, unknown, drop

# Example: Set destinations
destinations:
  postgresql_analytics:
    enabled: true
    database: "analytics_db"
    if_exists: "replace"  # replace, append, fail
```

## 📊 What This Demonstrates

### Technical Skills
- Python development
- YAML configuration
- Database connectivity (MySQL, PostgreSQL)
- REST API integration
- Data manipulation with Pandas
- Error handling and logging
- Professional documentation

### Professional Practices
- Modular, maintainable code
- Configuration-driven design
- Comprehensive logging
- Production-ready patterns
- Enterprise-grade error handling

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License.

## 👤 Author

**Blessing Onyekanna**
- GitHub: [@BlessingOnyekanna](https://github.com/BlessingOnyekanna)

## 🙏 Acknowledgments

- Built as part of IBM Data Engineering Professional Certificate
- Inspired by real-world data engineering challenges

## 📚 Related Projects

- [Insight Generator](https://github.com/BlessingOnyekanna/Insight-generator) - E-commerce analytics with PDF reports
- [Data Doctor](https://github.com/BlessingOnyekanna/Data-cleaning-portfolio) - Data cleaning and validation system

---

**⭐ If you find this project useful, please consider giving it a star!**