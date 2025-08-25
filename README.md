# Data Pipeline Sample

A production-ready ETL pipeline sample project demonstrating modern data engineering practices. This project showcases an API ingestion data pipeline with proper testing, logging, error handling, and database operations.

## Purpose

This repository serves as a **portfolio sample** for data engineering job applications, demonstrating:
- Modular, maintainable code structure
- Comprehensive testing (unit + integration)
- Proper logging and error handling
- Database abstraction and staging/production patterns
- Modern Python development practices

## 📁 Project Structure

```
data-pipeline-sample/
├── data_pipeline_sample/
│   ├── __init__.py
│   ├── config.py              # Configuration and setup 
│   ├── runner.py              # Pipeline orchestration 
│   ├── scripts/               
│   │   ├── __init__.py
│   │   ├── world_bank_ingestion.py  # World Bank API ingestion
│   │   └── db_loader.py             # Database loading with concurrency
│   ├── sql/
│   │   └── create_raw_tables.sql    # Database schema creation
│   └── utils/
│       ├── __init__.py
│       ├── utils.py                 # Base utility class
│       └── api_ingestion.py         # Generic API ingestion utilities
├── logs/                      # Log files 
├── data/raw/                  # Raw CSV data storage
├── tests/
│   ├── __init__.py
│   ├── test_world_bank_ingestion.py # World Bank data tests
│   ├── test_db_loader.py            # Database loading tests
│   └── test_api_ingestion.py        # API utilities tests
├── .env                       # Database credentials
├── poetry.lock
├── pyproject.toml
├── pytest.ini
└── run.py                     # Execution script
```

## 🛠️ Installation & Configuration

1. **Install dependencies**
   ```bash
   poetry install
   ```

2. **Configure environment variables**
   Create a `.env` file in the project root:
   ```bash
   # Database connection string (example for MySQL/MariaDB)
   ADDRESS_USER_DEV=mysql+pymysql://username:password@host:port
   ADDRESS_USER_PREPROD=mysql+pymysql://username:password@host:port
   ADDRESS_USER_PROD=mysql+pymysql://username:password@host:port
   ```
   Replace `username`, `password`, and server details with your actual database credentials.

3. **Update configuration**
   Modify `data_pipeline_sample/config.py` to match your environment:
   ```python
   ENV = "DEV"  # or "PREPROD", "PROD"
   DB_PROD = "your_production_database"
   ```

##  Usage

### Run a Specific Script

```bash
# Run World Bank energy data ingestion
poetry run python run.py world_bank

# Run database loading with concurrency
poetry run python run.py db_loader

# Run the complete pipeline
poetry run python run.py all
```

## Testing

### Run Tests

```bash
# Run all tests
poetry run pytest

# Run a specific test file
poetry run pytest tests/test_script_name.py
```
**Note:** Tests are executed automatically when scripts are run.


## Logs
Each script generates a log file with the same name in the logs directory during execution.

## World Bank Energy Data Integration

This project includes an energy data ingestion module using the World Bank API.

### Data Sources
- **World Bank API**: Multiple energy indicators (EG.ELC.FOSL.ZS, EG.ELC.RNEW.ZS, EG.ELC.NUCL.ZS, EG.ELC.ACCS.ZS)
- **Data Coverage**: Global energy data from 1960-2024
- **Format**: Structured CSV with country codes, indicators, and yearly values

## Key Features

- **SQLAlchemy Integration**: Uses standard SQLAlchemy for database operations
- **World Bank API Integration**: Demonstrates external API integration with energy data sources
- **Concurrent Processing**: ThreadPoolExecutor for efficient database loading
- **Staging/Production Pattern**: Safe data loading with staging environment
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **Data Validation**: Built-in testing framework for data quality
- **Error Handling**: Robust error handling and retry logic
- **Modular Design**: Clean separation of concerns and reusable components





