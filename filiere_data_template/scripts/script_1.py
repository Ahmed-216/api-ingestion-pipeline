import os
import pandas as pd
import filiere_data_template.config as cfg
from filiere_data_template.utils.base import BaseClass

# Get script name dynamically
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
# Set up logging
logger = cfg.setup_logging(SCRIPT_NAME)


class ScriptOne(BaseClass):
    """
    Template script to be used as a reference for new scripts.
    This script executes a query, filters the results and saves the data in a table.
    """

    def __init__(self) -> None:
        super().__init__(logger=logger.getChild("ingestion"))

    def run(self, run_tests: bool = True) -> None:
        """
        Get entity data and filter on french-based entities.
            1. Get entity data from the c4f_referantial database
            2. Filter on french-based entities
            3. Run unit tests to validate the transformed data (optional)
            4. Load the transformed data into database

        Args:
            run_tests: bool, whether to run data validation tests
                (default: True)
        """
        logger.info("Starting ScriptOne")

        # 1. Get data from source
        entity_data = self.read_file_as_df("query.sql")

        # 2. Transform/filter data
        french_entities = self.transform_data(df=entity_data)

        # 3. Run unit test to check the transformed data
        if run_tests:
            test_path = os.path.join(
                cfg.TEST_DIR,
                "test_" + SCRIPT_NAME + ".py",
            )
            self.run_unit_tests(test_path)

        # 4. Load to database
        self.load_data(df=french_entities, table_name="FrenchEntities")

        logger.info("✅ ScriptOne finished successfully")

    @staticmethod
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter data on french-based entities

        Args:
            df: Input DataFrame containing entity data

        Returns:
            Filtered DataFrame containing only French entities
        """
        logger.info("Transforming data")

        df = df[df["isoHq"] == "FR"]

        logger.info(f"Found {len(df)} french entities")

        return df
