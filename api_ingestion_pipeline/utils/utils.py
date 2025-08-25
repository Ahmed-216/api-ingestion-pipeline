import os
import logging
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
import subprocess
from typing import Optional
from dotenv import load_dotenv
import api_ingestion_pipeline.config as cfg
import pandas as pd

load_dotenv()


class BaseClass:
    """Base Class for common utility functions and database connections using SQLAlchemy"""

    def __init__(
        self,
        database_staging: str = cfg.DB_STAGING,
        database_prod: str = cfg.DB_PROD,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Set up database connections and logger using SQLAlchemy.
        """
        # get database credentials
        self.url = os.getenv(cfg.ENV_ADDRESS_USER)

        if not self.url:
            raise ValueError(f"Database URL not found in environment variable")

        # create engines for staging and production databases
        self.sql_staging = create_engine(f"{self.url}/{database_staging}")
        self.sql_prod = create_engine(f"{self.url}/{database_prod}")

        # create session factories
        self.SessionStaging = sessionmaker(bind=self.sql_staging)
        self.SessionProd = sessionmaker(bind=self.sql_prod)

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

        # Read SQL file
        with open(query_path, "r", encoding="utf-8") as file:
            query = file.read()

        # Execute query and return DataFrame
        data = pd.read_sql(query, self.sql_prod)

        self.logger.info(f"Retrieved {len(data)} records")
        if len(data) > 0:
            self.logger.info(f"Sample data:\n{data.head()}")

        return data

    def load_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Load data to database using pandas to_sql

        Args:
            df: data to load
            table_name: name of the table to load data to
        """
        self.logger.info("Loading data to database")

        # Load data to staging database
        df.to_sql(
            name=table_name,
            con=self.sql_staging,
            if_exists="replace",
            index=False,
            method="multi",
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

        # Create staging session
        with self.SessionStaging() as session:
            # Drop existing table in production if it exists
            drop_stmt = text(f"DROP TABLE IF EXISTS {cfg.DB_PROD}.{table_name}")
            session.execute(drop_stmt)

            # Rename table from staging to production
            rename_stmt = text(
                f"RENAME TABLE {cfg.DB_STAGING}.{table_name} TO {cfg.DB_PROD}.{table_name}"
            )
            session.execute(rename_stmt)

            session.commit()

        self.logger.info(
            f"Table {table_name} moved successfully from staging to production database"
        )

    def execute_query(self, query: str, engine: Optional[Engine] = None) -> None:
        """
        Execute a raw SQL query

        Args:
            query: SQL query to execute
            engine: SQLAlchemy engine to use (defaults to staging)
        """
        if engine is None:
            engine = self.sql_staging

        with engine.connect() as connection:
            connection.execute(text(query))
            connection.commit()

    def execute_sql_file(self, sql_file_path: str) -> None:
        """
        Execute a SQL file
        """
        self.logger.info(f"Executing SQL file: {sql_file_path}")

        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_content = file.read()

        # Split by semicolon and execute each statement separately
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

        with self.sql_staging.connect() as connection:
            for statement in statements:
                if statement:
                    try:
                        connection.execute(text(statement))
                        connection.commit()  # Commit each statement individually
                    except Exception as e:
                        self.logger.error(
                            f"Error executing statement: {statement[:100]}..."
                        )
                        raise e

        self.logger.info(f"✅ SQL file {sql_file_path} executed successfully")

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
