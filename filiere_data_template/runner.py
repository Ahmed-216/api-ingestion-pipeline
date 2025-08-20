from filiere_data_template.scripts.script_1 import ScriptOne
from filiere_data_template.scripts.script_2 import ScriptTwo

"""
Runner script to orchestrate the execution of the pipeline.
"""


class ScriptOneRunner:
    def run(self):
        script_one = ScriptOne()
        script_one.run()


class ScriptTwoRunner:
    def run(self):
        script_two = ScriptTwo()
        script_two.run()


class PipelineRunner:
    def run(self):
        print("Running pipeline")

        # Run script one
        script_one_runner = ScriptOneRunner()
        script_one_runner.run()

        # Run script two
        script_two_runner = ScriptTwoRunner()
        script_two_runner.run()

        print("✅ Pipeline finished successfully")
