import pytest
import pandas as pd
from filiere_data_template.scripts.script_1 import ScriptOne


class TestScriptOne:
    """
    Test class for ScriptOne functionality
    In this template test, we test the successful filtering of french entities
    using two different approaches for demonstration purposes:
        1. Unit test for transform_data method using mocked data
        2. Integration test that tests the real data obtained from french_entities
    """

    @pytest.fixture
    def script_one(self) -> ScriptOne:
        """Fixture to create ScriptOne instance"""
        return ScriptOne()

    @pytest.fixture
    def sample_entity_data(self) -> pd.DataFrame:
        """Fixture providing sample entity data for testing"""
        return pd.DataFrame(
            {
                "c4fEntityId": [1, 2, 3, 4, 5],
                "entityName": [
                    "Company A",
                    "Company B",
                    "Company C",
                    "Company D",
                    "Company E",
                ],
                "isoHq": ["FR", "US", "FR", "DE", "FR"],
            }
        )

    def test_transform_data_filters_french_entities_only(
        self, script_one: ScriptOne, sample_entity_data: pd.DataFrame
    ) -> None:
        """
        Unit test for transform_data method using mocked data.
        Tests that the method correctly filters only French entities
        (isoHq == 'FR').
        """
        result = script_one.transform_data(sample_entity_data)

        assert list(result["c4fEntityId"]) == [1, 3, 5]

    def test_filtered_data(self, script_one: ScriptOne) -> None:
        """
        Integration test that tests the real data obtained from french_entities.
        """
        entity_data = script_one.read_file_as_df("query.sql")
        french_entities = script_one.transform_data(df=entity_data)

        assert len(french_entities) > 0
        assert all(french_entities["isoHq"] == "FR")
