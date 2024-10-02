"""Test custom validator for even integer."""

import logging

import pandas as pd
import pytest
from pydantic import BaseModel, field_validator

from pandantic import Pandantic


logging.basicConfig(level=logging.DEBUG)


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    example_str: str
    example_int: int

    @field_validator("example_int")
    def validate_even_integer(cls, x: int) -> int:  # pylint: disable=invalid-name, no-self-argument
        """Example custom validator to validate if int is even."""
        if x % 2 != 0:
            raise ValueError(f"example_int must be even, is {x}.")
        return x


def test_custom_validator_even_pass():
    """Test that a custom validator passes."""

    # GIVEN
    example_df_valid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", "baz"],
            "example_int": [2, 4, 12],
        }
    )

    validator = Pandantic(schema=DataFrameSchema)

    # WHEN
    df_valid = validator.validate(
        dataframe=example_df_valid,
        errors="filter",
    )

    # THEN
    assert df_valid.equals(example_df_valid)


def test_custom_validator_even_fail_filter():
    """Test that a custom validator fails."""

    # GIVEN
    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", "baz"],
            "example_int": [1, 4, 12],
        }
    )

    validator = Pandantic(schema=DataFrameSchema)

    # WHEN
    df_invalid = validator.validate(
        dataframe=example_df_invalid,
        errors="filter",
    )

    # THEN
    assert df_invalid.equals(example_df_invalid.drop(index=[0]))


def test_custom_validator_even_fail_raise():
    """Test that a custom validator fails."""

    # GIVEN
    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", "baz"],
            "example_int": [1, 4, 12],
        }
    )

    validator = Pandantic(schema=DataFrameSchema)

    # THEN
    with pytest.raises(ValueError):
        # WHEN
        validator.validate(
            dataframe=example_df_invalid,
            errors="raise",
        )
