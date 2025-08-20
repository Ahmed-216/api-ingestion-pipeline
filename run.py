"""
Unified Pipeline Runner
Runs either entire pipeline or specific script

Usage:
    poetry run python run.py script_1
    poetry run python run.py script_2
    poetry run python run.py all
"""

import argparse
import filiere_data_template.runner as pipeline_runner


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
            "script_1",
            "script_2",
            "all",
        ],
        help="Pipeline script to run",
    )

    args = parser.parse_args()

    # script mapping
    scripts = {
        "script_1": pipeline_runner.ScriptOneRunner,
        "script_2": pipeline_runner.ScriptTwoRunner,
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
