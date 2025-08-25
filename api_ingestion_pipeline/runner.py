from api_ingestion_pipeline.scripts.world_bank_ingestion import WorldBankIngestion
from api_ingestion_pipeline.scripts.db_loader import DBLoader

"""
Runner script to orchestrate the execution of the pipeline.
"""


class WorldBankRunner:
    def run(self):
        world_bank_pipeline = WorldBankIngestion()
        world_bank_pipeline.run()


class DBLoaderRunner:
    def run(self):
        db_loader = DBLoader()
        db_loader.run()


class PipelineRunner:
    def run(self):
        print("Running pipeline")

        # Run World Bank energy ingestion
        world_bank_runner = WorldBankRunner()
        world_bank_runner.run()

        # Run database loader
        db_loader_runner = DBLoaderRunner()
        db_loader_runner.run()

        print("✅ Pipeline finished successfully")
