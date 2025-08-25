from api_ingestion_pipeline.utils.api_ingestion import ApiIngestion
from api_ingestion_pipeline.utils.utils import BaseClass
import api_ingestion_pipeline.config as cfg
import pandas as pd
from typing import Dict, List, Optional
import os

# Get script name dynamically
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]

# Set up logging
logger = cfg.setup_logging(SCRIPT_NAME)


class WorldBankIngestion(ApiIngestion, BaseClass):
    """
    Ingest data from World Bank API and save as CSV in a specified directory
    """

    def __init__(self, output_dir: str = None, headers: Optional[Dict] = None):
        self.output_dir = output_dir or os.path.join(cfg.ROOT_DIR, "data", "raw")

        # Initialize ApiIngestion
        ApiIngestion.__init__(
            self,
            output_dir=self.output_dir,
            headers=headers,
            logger=logger.getChild("ingestion"),
        )
        # Initialize BaseClass
        BaseClass.__init__(self, logger=logger.getChild("external_data"))

        # Default parameters for World Bank API
        self.params = {"format": "json", "per_page": 1000}

    def run(self, run_tests: bool = True):
        """
        Download data from World Bank API and save as CSV in a specified
        directory:
            1. Get data sources API endpoints
            2. Clean up existing World Bank CSV files
            3. Download the data from the API endpoints
            4. Transform the data to match the expected format
            5. Save csv files in the output directory
            6. Run validation tests (optional)

        Args:
            run_tests: bool, whether to run data validation tests
                (default: True)
        """
        # Clear the log file at the start of each run
        self.clear_log_file(SCRIPT_NAME)

        logger.info(" Starting World Bank data ingestion process...")

        # 1. Get data sources API endpoints
        data_sources = cfg.DATA_SOURCES["world_bank"]
        data_sources_list = [
            source_config["filename"] for source_config in data_sources.values()
        ]

        # 2. Clean up existing World Bank CSV files
        self.cleanup_existing_files(data_sources_list)

        # 3. Download the data from the API endpoints
        fetched_data = {}
        for source_name, source_config in data_sources.items():
            logger.info(f"Downloading {source_name}: {source_config['description']}")

            # Merge default params with any custom params
            params = self.params.copy()
            if "params" in source_config:
                params.update(source_config["params"])

            # Use parent class method to fetch the data
            fetched_data[source_name] = self.fetch_paginated_data(
                base_url=source_config["url"],
                params=params,
                api_description=source_name,
            )

        logger.info("✅ All downloads completed successfully!")

        # 4. Transform the data
        logger.info("Transforming raw data to match expected format")
        transformed_data = self._transform_data(fetched_data)

        # 5. Save csv files in the output directory
        for source_name, df in transformed_data.items():
            source_config = data_sources[source_name]
            logger.info(f"Saving {source_name}: {source_config['description']}")

            file_path = os.path.join(
                self.output_dir, source_config["filename"] + ".csv"
            )
            df.to_csv(file_path, index=False)
            logger.info(f"✅ Saved {source_config['filename']}")

        logger.info("✅ Data transformation and saving completed successfully!")

        # 6. Run validation tests (optional)
        if run_tests:
            test_path = os.path.join(
                cfg.TEST_DIR,
                "test_" + SCRIPT_NAME + ".py",
            )
            self.run_unit_tests(test_path)

    @staticmethod
    def _transform_data(data: Dict[str, List[Dict]]) -> Dict[str, pd.DataFrame]:
        """Transform World Bank API data into expected CSV format."""
        transformed_data = {}

        for source_name, records_list in data.items():

            logger.info(f"Transforming {source_name} data...")

            # Extract records from API response
            records = []
            for record in records_list:
                if isinstance(record, dict) and "value" in record:
                    records.append(
                        {
                            "Country Code": record.get("countryiso3code"),
                            "Country Name": record.get(
                                "country",
                                {},
                            ).get("value", ""),
                            "Indicator Name": record.get(
                                "indicator",
                                {},
                            ).get(
                                "value",
                                "",
                            ),
                            "Indicator Code": record.get(
                                "indicator",
                                {},
                            ).get("id", ""),
                            "Year": record.get("date", ""),
                            "Value": record.get("value", ""),
                        }
                    )

            if not records:
                logger.warning(f"No valid records found for {source_name}")
                continue

            # Create DataFrame
            df = pd.DataFrame(records)

            # Clean and validate data
            if not df.empty:
                # Convert to numeric, handling errors gracefully
                df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
                df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

                # Remove rows with missing country codes or invalid data
                df = df[df["Country Code"].notna() & (df["Country Code"] != "")]
                df = df[df["Year"].notna()]

                # Sort for consistent output
                df = df.sort_values(["Country Code", "Year"])

            if df.empty:
                logger.warning(f"No valid data after cleaning for {source_name}")
                continue

            # Pivot to wide format (years as columns)
            try:
                df_wide = df.pivot(
                    index=[
                        "Country Code",
                        "Country Name",
                        "Indicator Code",
                        "Indicator Name",
                    ],
                    columns="Year",
                    values="Value",
                )
                df_wide.reset_index(inplace=True)

                transformed_data[source_name] = df_wide
                logger.info(
                    f"✅ Transformed {source_name} with {len(df_wide)} countries"
                )

            except Exception as e:
                logger.error(f"❌ Failed to transform {source_name}: {str(e)}")
                continue

        return transformed_data
