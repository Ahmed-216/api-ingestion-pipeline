import os
import filiere_data_template.config as cfg
from filiere_data_template.utils.base import BaseClass

# Get script name dynamically
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
# Set up logging
logger = cfg.setup_logging(SCRIPT_NAME)


class ScriptTwo(BaseClass):
    """
    Second template script used to demonstrate how we orchestrate a data pipeline
    with multiple scripts using the runner.py file.
    """

    def __init__(self):
        super().__init__(logger=logger.getChild("ingestion"))

    def run(self):
        """
        We simply log a message as basic transformations and database connections
        were already demonstrated in script_1
        """
        logger.info("script_2 executed succesfully")
