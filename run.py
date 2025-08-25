"""
Unified Pipeline Runner
Runs either entire pipeline or specific script

Usage:
    poetry run python run.py world_bank
    poetry run python run.py db_loader
    poetry run python run.py all
"""

import argparse
import api_ingestion_pipeline.runner as pipeline_runner


def main():
    """
    Run the data pipeline or specific script given the user input.
    """

    parser = argparse.ArgumentParser(
        description="Run data pipeline scripts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "script",
        choices=[
            "world_bank",
            "db_loader",
            "all",
        ],
        help="Pipeline script to run",
    )

    args = parser.parse_args()

    # script mapping
    scripts = {
        "world_bank": pipeline_runner.WorldBankRunner,
        "db_loader": pipeline_runner.DBLoaderRunner,
        "all": pipeline_runner.PipelineRunner,
    }

    # Get the script
    script = scripts[args.script]

    print(f"Running {args.script}...")

    runner = script()
    runner.run()
    print(f"✅ {args.script} completed successfully!")


if __name__ == "__main__":
    main()
