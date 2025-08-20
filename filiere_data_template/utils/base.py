import os
import logging
from sqlalchemy import text
from de_tools.sql_connection import MariaConnection
import subprocess
from typing import Optional, List
from dotenv import load_dotenv
import filiere_data_template.config as cfg
import pandas as pd

load_dotenv()


class BaseClass(MariaConnection):
    """Base Class for common utility functions and database connections"""

    def __init__(
        self,
        database_staging: str = cfg.DB_STAGING,
        database_prod: str = cfg.DB_PROD,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Set up database connections and logger.
        """
        # get databse credentials
        self.url = os.getenv(cfg.ENV_ADDRESS_USER)

        # connect to staging database
        self.sql_staging: MariaConnection = MariaConnection(
            url=self.url,
            database=database_staging,
        )

        # connect to production database
        self.sql_prod: MariaConnection = MariaConnection(
            url=self.url,
            database=database_prod,
        )

        # set logger
        self.logger = logger if logger is not None else logging.getLogger(__name__)

    def read_file_as_df(self, query_name: str) -> pd.DataFrame:
        """
        Read a query file from the sql directory and return a pandas dataframe.

        Args:
            query_name: Name of the query file

        Returns:
            DataFrame containing the query results
        """
        self.logger.info(f"Executing query: {query_name}")

        query_path = os.path.join(cfg.SQL_DIR, query_name)

        query = self.sql_prod.read_sql_file(query_path)

        data = self.sql_prod.read_query_as_df(query)

        self.logger.info(f"Retrieved {len(data)} records \n {data.head()}")

        return data

    def load_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Load data to database

        Args:
            df: data to load
            table_name: name of the table to load data to
        """
        self.logger.info("Loading data to database")

        self.sql_staging.df_to_sql(
            df=df,
            table_name=table_name,
            index=False,
            if_exists="append",
        )

        self.move_table_from_staging(table_name=table_name)

        self.logger.info("Data loaded successfully")

    def move_table_from_staging(self, table_name: str) -> None:
        """
        Move a table from staging to production database

        Args:
            table_name: name of the table to move
        """

        self.logger.info("Moving a table from staging to production database")

        drop_stmt = text(f"DROP TABLE IF EXISTS {cfg.DB_PROD}.{table_name}")
        rename_stmt = text(
            f"RENAME TABLE {cfg.DB_STAGING}.{table_name} TO {cfg.DB_PROD}.{table_name}"
        )
        self.sql_staging.execute_query(drop_stmt)
        self.sql_staging.execute_query(rename_stmt)

        self.logger.info(
            f"Table {table_name} moved successfully from staging to production database"
        )

    def run_unit_tests(self, test_path: str) -> None:
        """
        Run unit tests for a given script and personalize the output.
        This function uses pytest to execute the tests and logs the output.

        Args:
            test_path: path to the test file to run.

        Raises:
            RuntimeError: If any tests fail, a RuntimeError is raised
                with details.
        """
        self.logger.info(f"Running unit tests: {test_path}")

        # Build test command
        test_command = ["pytest"]
        test_command.append(test_path)
        test_command.extend(["--color=no"])

        try:
            # Run tests and capture output
            result = subprocess.run(
                test_command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=True,  # Raises CalledProcessError on failure
            )

            # Log success with summary
            success_summary = [
                line
                for line in result.stdout.splitlines()
                if "passed" in line and "=" in line
            ][
                -1
            ]  # Get last summary line

            self.logger.info(f"✅ All tests passed\n{success_summary}")

        except subprocess.CalledProcessError as e:
            # Extract failed test names
            failed_tests = [
                line.split("::")[1].split()[0].strip()
                for line in e.stdout.splitlines()
                if "FAILED" in line
            ]

            # Log failure details
            self.logger.error(
                f"❌ {len(failed_tests)} tests failed:\n"
                + "\n".join(f"• {name}" for name in failed_tests)
            )

            raise RuntimeError(
                "Unit tests failed - Aborting script - See logs for details"
            )
