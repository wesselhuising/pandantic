"""Test custom validator context."""
import logging

import pandas as pd
import pytest
from pydantic import ValidationError, field_validator

from pandantic import BaseModel


logging.basicConfig(level=logging.DEBUG)

COUNTRY_LIST = ["USA", "UK", "CANADA"]


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    example_str: str
    example_int: int

    @field_validator("example_str")
    def validate_country_in_list(  # pylint: disable=invalid-name, no-self-argument
        cls, x: str
    ) -> int:
        """Example custom validator to validate if int is even."""
        if x not in COUNTRY_LIST:
            raise ValidationError(f"example_str must be part of country list, is {x}.")
        return x


def test_custom_validator_context_pass():
    """Test that a custom validator passes."""

    # GIVEN
    example_df_valid = pd.DataFrame(
        data={
            "example_str": ["USA", "UK", "CANADA"],
            "example_int": [2, 4, 12],
        }
    )

    # WHEN
    df_valid = DataFrameSchema.parse_df(
        dataframe=example_df_valid,
        errors="filter",
    )

    # THEN
    assert df_valid.equals(example_df_valid)


def test_custom_validator_context_fail_filter():
    """Test that a custom validator fails."""

    # GIVEN
    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["USA", "UK", "NETHERLANDS"],
            "example_int": [1, 4, 12],
        }
    )

    # WHEN
    df_invalid = DataFrameSchema.parse_df(
        dataframe=example_df_invalid,
        errors="filter",
    )

    # THEN
    assert df_invalid.equals(example_df_invalid.drop(index=[2]))


def test_custom_validator_context_fail_raise():
    """Test that a custom validator fails."""

    # GIVEN
    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["USA", "UK", "NETHERLANDS"],
            "example_int": [1, 4, 12],
        }
    )

    # THEN
    with pytest.raises(ValueError):
        # WHEN
        DataFrameSchema.parse_df(
            dataframe=example_df_invalid,
            errors="raise",
        )
