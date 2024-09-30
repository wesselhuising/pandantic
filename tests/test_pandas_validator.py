"""Tests the core functionality of PandasValidator using a more complex
pydantic model w/ a custom int and str column validator:
    * validate() function (full table).
    * validate() function (to filter table).
    * iterate() function.
"""
import logging
from typing import Optional

import pandas as pd
import pytest
from pydantic import BaseModel, ValidationError, field_validator

from pandantic.validators.pandas import PandasValidator


logging.basicConfig(level=logging.DEBUG)

COUNTRY_LIST = ["USA", "UK", "CANADA"]


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    example_str: str
    example_int: int

    @field_validator("example_int")
    def validate_even_integer(cls, x: int) -> int:  # pylint: disable=invalid-name, no-self-argument
        """Example custom validator to validate if int is even."""
        if x % 2 != 0:
            raise ValidationError(f"example_int must be even, is {x}.")
        return x

    @field_validator("example_str")
    def validate_country_in_list(  # pylint: disable=invalid-name, no-self-argument
        cls, x: str
    ) -> str:
        """Example custom validator to validate if int is even."""
        if x not in COUNTRY_LIST:
            raise ValidationError(f"example_str must be part of country list, is {x}.")
        return x


@pytest.fixture
def validator() -> PandasValidator:
    """Fixture for a simple dataframe."""
    return PandasValidator(schema=DataFrameSchema)


def test_custom_validator_pass(validator: PandasValidator):
    """Test that a custom validator passes."""

    # GIVEN
    valid_df = pd.DataFrame(
        data={
            "example_str": ["USA", "UK", "CANADA"],
            "example_int": [2, 4, 12],
        }
    )

    # WHEN -> THEN
    result = validator.validate(valid_df, errors="filter")
    assert result.equals(valid_df)

    result = validator.validate(valid_df, errors="filter", n_jobs=2)
    assert result.equals(valid_df)


def test_custom_str_validator_fail(validator: PandasValidator):
    """Test that a custom validator fails."""

    # GIVEN
    int_invalid_df = pd.DataFrame(
        data={
            "example_str": ["USA", "UK", "CANADA"],
            "example_int": [1, 4, 12],
        }
    )

    # WHEN -> THEN
    result = validator.validate(int_invalid_df, errors="filter")
    assert result.equals(int_invalid_df.drop(index=[0]))

    result = validator.validate(int_invalid_df, errors="filter", n_jobs=2)
    assert result.equals(int_invalid_df.drop(index=[0]))


def test_custom_int_validator_fail(validator: PandasValidator):
    """Test that a custom validator fails."""
    # GIVEN
    str_invalid_df = pd.DataFrame(
        data={
            "example_str": ["foo", "UK", "CANADA"],
            "example_int": [2, 4, 12],
        },
    )

    # WHEN -> THEN
    result = validator.validate(str_invalid_df, errors="filter")
    assert result.equals(str_invalid_df.drop(index=[0]))

    result = validator.validate(str_invalid_df, errors="filter", n_jobs=2)
    assert result.equals(str_invalid_df.drop(index=[0]))


def test_custom_validator_fail_raise(validator: PandasValidator):
    """Test that a custom validator fails."""

    # GIVEN
    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", "baz"],
            "example_int": [1, 4, 12],
        }
    )

    # THEN
    with pytest.raises(ValueError):
        # WHEN
        validator.validate(
            dataframe=example_df_invalid,
            errors="raise",
        )


def test_optional_int_parse_df_with_default():
    """Test that an optional int with a default value is set to None when not provided."""

    # GIVEN
    class Model(BaseModel):
        a: Optional[int] = None
        b: int

    df_example = pd.DataFrame({"a": [1, None, 2], "b": ["str", 2, 3]})
    validator = PandasValidator(schema=Model)

    # WHEN
    df_filtered = validator.validate(df_example, errors="filter", verbose=True)

    # THEN
    assert len(df_filtered) == 1


def test_optional_int_parse_df_all_none():
    # GIVEN
    class Model(BaseModel):
        a: Optional[int] = None
        b: str

    df_example = pd.DataFrame({"a": [None, None, None], "b": ["str", "str", "str"]})
    validator = PandasValidator(schema=Model)

    # WHEN
    df_filtered = validator.validate(df_example, errors="filter", verbose=True)

    # THEN
    assert df_filtered.equals(df_example)


def test_iterate():
    ...
