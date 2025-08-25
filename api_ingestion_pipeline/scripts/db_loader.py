import api_ingestion_pipeline.config as cfg
from api_ingestion_pipeline.utils.utils import BaseClass
import pandas as pd
import os
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Get script name dynamically
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]

# Set up logging
logger = cfg.setup_logging(SCRIPT_NAME)


class DBLoader(BaseClass):
    """Class to load raw data from csv files into sql database."""

    def __init__(self):
        super().__init__(logger=logger.getChild("base"))
        self.data_dir = cfg.RAW_DATA_DIR
        self.csv_names_list = [
            source["filename"]
            for source in list(cfg.DATA_SOURCES["world_bank"].values())
        ]

    def _load_table_data(self, table_name: str, df: pd.DataFrame) -> str:
        """Load a single table's data."""
        try:
            self.load_data(df, table_name)
            return f"✅ Successfully loaded {table_name}"
        except Exception as e:
            return f"❌ Failed to load {table_name}: {str(e)}"

    def run(self, run_tests: bool = True):
        """
        Load ingested raw data from csv files into sql database.
            1. Create raw tables.
            2. Extract raw data from csv files.
            3. Load raw data into created tables.
            4. Run validation tests to ensure the data is loaded correctly.
        """
        logger.info("Start Data loading process..")

        # 1. Create raw tables.
        sql_file_path = os.path.join(cfg.SQL_DIR, "create_raw_tables.sql")
        self.execute_sql_file(sql_file_path)

        # 2. Extract raw data from csv files.
        inputs_df = self._read_input_csv()

        # 3. Load raw data into created tables with concurrency.
        self._load_tables_concurrent(inputs_df, max_workers=4)

        # 5. Run validation tests to ensure the data is loaded correctly.
        if run_tests:
            logger.info("Running data quality tests...")
            test_path = os.path.join(
                cfg.TEST_DIR,
                "test_" + SCRIPT_NAME + ".py",
            )
            self.run_unit_tests(test_path)
            logger.info("✅ All data quality tests passed!")

    def _read_input_csv(
        self,
        csv_files_path: Optional[str] = None,
        csv_names_list: Optional[List[str]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Create a dictionary of DataFrames based on all raw CSV inputs.

        Args:
            csv_files_path (str, optional):
                Path to the folder where raw CSV input files are located.
                Defaults to self.data_dir.
            csv_names_list (list, optional):
                List of CSV filenames.
                Defaults to self.csv_names_list.

        Returns:
            dict: Dictionary of DataFrames.
        """
        logger.info("Reading CSV files..")

        inputs_df: Dict[str, pd.DataFrame] = {}
        path = csv_files_path or self.data_dir
        csv_names = csv_names_list or self.csv_names_list

        for file in csv_names:
            try:
                logger.info(f"Reading CSV file: {file}")
                inputs_df[file] = pd.read_csv(
                    os.path.join(path, file + ".csv"),
                    encoding="utf-8",
                )
            except FileNotFoundError:
                logger.error(f"File not found: {os.path.join(path, file)}")
                continue
            except Exception as e:
                logger.error(f"Error reading CSV file {file}: {e}")
                raise

        return inputs_df

    def _load_tables_concurrent(
        self, inputs_df: Dict[str, pd.DataFrame], max_workers: int
    ) -> None:
        """
        Load raw data into created tables with concurrency.

        Args:
            inputs_df (dict): Dictionary of DataFrames.
            max_workers (int): Maximum number of concurrent threads.
        """
        logger.info(
            f"Loading {len(inputs_df)} tables with {max_workers} "
            f"concurrent threads..."
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(self._load_table_data, table_name, df): table_name
                for table_name, df in inputs_df.items()
            }

            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    logger.info(result)
                except Exception as e:
                    logger.error(f"❌ Exception loading {table_name}: {str(e)}")
