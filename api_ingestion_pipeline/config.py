import os
import logging

# Specify the environment (DEV, PREPROD or PROD)
ENV = "DEV"
ENV_ADDRESS_USER = f"ADDRESS_USER_{ENV}"

# Database connection parameters
DB_STAGING = "staging"
DB_PROD = "sandbox"  # Replace with relevant production database

# Project root directory
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Path to the raw data directory (relative to project root)
RAW_DATA_DIR = os.path.join(ROOT_DIR, "data")

# Path to the SQL files (relative to project root)
SQL_DIR = os.path.join(ROOT_DIR, "api_ingestion_pipeline", "sql")

# Test directory (relative to project root)
TEST_DIR = os.path.join(ROOT_DIR, "tests")

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# API endpoints
DATA_SOURCES = {
    "world_bank": {
        "fossil_fuel_electricity": {
            "url": "https://api.worldbank.org/v2/country/all/indicator/EG.ELC.FOSL.ZS",
            "description": "Electricity production from fossil fuels (% of total)",
            "filename": "fossil_fuel_electricity",
        },
        "renewable_electricity": {
            "url": "https://api.worldbank.org/v2/country/all/indicator/EG.ELC.RNEW.ZS",
            "description": "Electricity production from renewable sources (% of total)",
            "filename": "renewable_electricity",
        },
        "nuclear_electricity": {
            "url": "https://api.worldbank.org/v2/country/all/indicator/EG.ELC.NUCL.ZS",
            "description": "Electricity production from nuclear sources (% of total)",
            "filename": "nuclear_electricity",
        },
    },
}


def setup_logging(script_name: str) -> logging.Logger:
    """
    Set up a per-script logger that writes to logs/{script_name}.log and console,

    Args:
        script_name: Name of the script (used for log filename)

    Returns:
        Configured logger instance
    """
    # Create absolute path for logs directory (relative to project root)
    logs_dir = os.path.join(ROOT_DIR, "logs")

    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)

    # Create log filename
    log_filename = f"{script_name}.log"
    log_filepath = os.path.join(logs_dir, log_filename)

    # Configure named logger
    logger: logging.Logger = logging.getLogger(script_name)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    logger.propagate = False

    # Remove existing handlers to avoid duplicates
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler(
        log_filepath,
        mode="a",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
