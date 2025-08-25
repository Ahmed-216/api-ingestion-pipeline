import os
import pytest
import pandas as pd
from datetime import datetime
import api_ingestion_pipeline.config as cfg
from api_ingestion_pipeline.scripts.world_bank_ingestion import WorldBankIngestion


@pytest.fixture(scope="module")
def data_dir():
    """Get the actual data directory from config."""
    return os.path.join(cfg.ROOT_DIR, "data", "raw")


@pytest.fixture(scope="module")
def world_bank_ingestion(data_dir):
    """Create World Bank ingestion instance."""
    return WorldBankIngestion(output_dir=data_dir)


@pytest.fixture(
    params=list(cfg.DATA_SOURCES["world_bank"].items()),
    ids=[name for name, _ in cfg.DATA_SOURCES["world_bank"].items()],
)
def world_bank_params(request, data_dir):
    """Parametrized case: (source_name, source_config, expected_file_path)."""
    source_name, source_config = request.param
    expected_file_path = os.path.join(
        data_dir,
        source_config["filename"] + ".csv",
    )
    return source_name, source_config, expected_file_path


def test_all_expected_csv_files_exist(world_bank_params):
    """Test that each expected CSV file is present."""
    source_name, source_config, expected_file_path = world_bank_params
    assert os.path.exists(expected_file_path), (
        f"Expected CSV file not found: {source_config['filename']}.csv "
        f"for source: {source_name}. "
        f"Make sure you have run the World Bank ingestion script first."
    )


def test_csv_files_have_content(world_bank_params):
    """Test that each CSV file has content."""
    _, source_config, expected_file_path = world_bank_params
    file_size = os.path.getsize(expected_file_path)
    assert file_size > 0, (
        f"CSV file is empty: {source_config['filename']}.csv "
        f"(size: {file_size} bytes)"
    )


def test_csv_files_have_correct_structure(world_bank_params):
    """Test that each CSV file has the correct column structure."""
    _, source_config, expected_file_path = world_bank_params

    # Base columns that should always be present
    base_columns = [
        "Country Code",
        "Country Name",
        "Indicator Code",
        "Indicator Name",
    ]

    # Generate expected year columns (1960 to current year - 1 based on World Bank data)
    current_year = datetime.now().year
    year_columns = [str(year) for year in range(1960, current_year)]
    expected_columns = base_columns + year_columns

    try:
        df = pd.read_csv(expected_file_path)
    except Exception as e:
        pytest.fail(
            f"Failed to read CSV file {source_config['filename']}.csv: {str(e)}"
        )

    missing_columns = set(expected_columns) - set(df.columns)
    extra_columns = set(df.columns) - set(expected_columns)

    assert (
        len(missing_columns) == 0
    ), f"Missing expected columns in {source_config['filename']}.csv: {missing_columns}"

    assert (
        len(extra_columns) == 0
    ), f"Unexpected columns in {source_config['filename']}.csv: {extra_columns}"


def test_csv_files_have_correct_data_types(world_bank_params):
    """Test that CSV files have correct data types for safe database loading."""
    _, source_config, expected_file_path = world_bank_params
    df = pd.read_csv(expected_file_path)

    # Check that string columns are object type (for database VARCHAR/TEXT)
    string_columns = [
        "Country Code",
        "Country Name",
        "Indicator Code",
        "Indicator Name",
    ]
    for col in string_columns:
        assert df[col].dtype == "object", (
            f"Column {col} is not object type in {source_config['filename']}.csv. "
            f"Got: {df[col].dtype}. Database loading may fail."
        )

    # Check that year columns (numeric years as column names) are numeric
    year_columns = df.columns[4:]
    for col in year_columns:
        # Try to convert to numeric to verify it's a year column
        try:
            year_value = int(col)
            assert 1900 <= year_value <= 2100, (
                f"Column {col} in {source_config['filename']}.csv doesn't appear to be a valid year. "
                f"Expected year between 1900-2100, got: {year_value}"
            )
            # Check that the column data is numeric
            assert pd.api.types.is_numeric_dtype(df[col]), (
                f"Year column {col} is not numeric in {source_config['filename']}.csv. "
                f"Database loading may fail."
            )
        except ValueError:
            pytest.fail(
                f"Column {col} in {source_config['filename']}.csv is not a valid year column. "
                f"Expected numeric year, got: {col}"
            )


def test_each_country_appears_once(world_bank_params):
    """Test that each country appears only once in each CSV file."""
    _, source_config, expected_file_path = world_bank_params
    df = pd.read_csv(expected_file_path)

    duplicate_countries = df["Country Code"].duplicated()
    assert not duplicate_countries.any(), (
        f"Duplicate countries found in {source_config['filename']}.csv: "
        f"{df[duplicate_countries]['Country Code'].tolist()}"
    )


def test_minimum_country_coverage(world_bank_params):
    """Test that we have data for at least 200 countries."""
    _, source_config, expected_file_path = world_bank_params
    df = pd.read_csv(expected_file_path)

    country_count = len(df)
    assert country_count >= 200, (
        f"Insufficient country coverage in {source_config['filename']}.csv. "
        f"Expected >= 200 countries, got: {country_count}"
    )


@pytest.mark.parametrize(
    "world_bank_params",
    [(n, c) for n, c in cfg.DATA_SOURCES["world_bank"].items()],
    ids=[n for n, c in cfg.DATA_SOURCES["world_bank"].items()],
    indirect=True,
)
def test_energy_data_within_expected_range(world_bank_params):
    """
    Test that energy data values are within 0-100% range.
    """
    _, source_config, expected_file_path = world_bank_params
    df = pd.read_csv(expected_file_path)

    # Get year columns (skip first 4 string columns) and filter to 2010 onwards
    year_columns = [col for col in df.columns[4:] if int(col) >= 2010]

    for col in year_columns:
        numeric_values = pd.to_numeric(df[col], errors="coerce").dropna()

        if len(numeric_values) > 0:
            # Check for values > 100 (100% of energy production/access)
            values_above_100 = (numeric_values > 100).sum()
            if values_above_100 > 0:
                # Log the countries with values > 100% for reference
                above_100_indices = numeric_values[numeric_values > 100].index
                countries_above_100 = df.loc[
                    above_100_indices, ["Country Code", "Country Name", col]
                ]
                print(
                    f"\nCountries with energy data > 100% in {col} "
                    f"of {source_config['filename']}.csv:"
                )
                print(countries_above_100.to_string())

            # Check that values are within reasonable range (0-100%)
            assert values_above_100 == 0, (
                f"Found {values_above_100} values > 100% in {col} "
                f"of {source_config['filename']}.csv. "
                f"Energy data percentages should be 0-100%."
            )


def test_unique_and_no_null_country_code(world_bank_params):
    """Test that no country code is null in any CSV file."""
    _, source_config, expected_file_path = world_bank_params
    df = pd.read_csv(expected_file_path)

    assert (
        not df["Country Code"].isnull().any()
    ), f"Null country code found in {source_config['filename']}.csv."

    assert df["Country Code"].nunique() == len(
        df
    ), f"Duplicate country codes found in {source_config['filename']}.csv."
