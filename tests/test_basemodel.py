"""Test the BaseModel class."""
import pandas as pd
import pytest
from pydantic.types import StrictInt

from pandantic import BaseModel


def test_dataframe_invalid_raise():
    """Test that an invalid dataframe raises a ValueError."""

    # GIVEN
    class DataFrameSchema(BaseModel):
        """Example schema for testing."""

        example_str: str
        example_int: int

    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", 1],
            "example_int": ["1", 2, 3.0],
        }
    )

    # THEN
    with pytest.raises(ValueError):
        # WHEN
        DataFrameSchema.parse_df(
            dataframe=example_df_invalid,
            errors="raise",
        )


def test_dataframe_invalid_filter_strict_int():
    """Test that an invalid dataframe filters correct rows using errors="filter"."""

    # GIVEN
    class DataFrameSchema(BaseModel):
        """Example schema for testing."""

        example_str: str
        example_int: StrictInt

    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", 1],
            "example_int": ["1", 2, 3.0],
        }
    )

    # GIVEN
    df_valid_filtered = DataFrameSchema.parse_df(
        dataframe=example_df_invalid,
        errors="filter",
    )

    # THEN
    assert df_valid_filtered.equals(example_df_invalid.drop(index=[0, 2]))
