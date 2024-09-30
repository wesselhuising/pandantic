import pytest
import pandas as pd
from typing import Optional
from pydantic import BaseModel, ValidationError

from pandantic.validators.pandas import PandasValidator

def test_validate_chunk():
    ...

def test_validate_single_job():
    ...

def test_validate_multiprocessing():
    ...


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
    assert df_filtered.equals(df_example.drop(index=[0]))


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

def tests_filter_multiprocessing():
    ...

def test_iterate():
    ...
