"""Tests the pandas plugin."""
import pandas as pd
import pytest
from pydantic import BaseModel
from pydantic.types import StrictInt


class DataFrameSchema1(BaseModel):
    """Example schema for testing."""

    str_col: str
    int_col: int
    strict_int: StrictInt


class DataFrameSchema2(BaseModel):
    """Example schema for testing."""

    str_col: str
    float_col: float


class DataFrameSchema3(BaseModel):
    """Example schema for testing."""

    str_col: str


@pytest.fixture
def dataframe() -> pd.DataFrame:
    """Fixture for a simple dataframe."""
    return pd.DataFrame(
        data={
            "str_col": ["foo", "bar", "baz"],
            "int_col": [1.0, 2.0, 3],
            "strict_int": [1, 2, 3],
        }
    )


def test_plugin(dataframe: pd.DataFrame):
    """Test the plugin."""
    import pandantic.plugins.pandas

    assert getattr(dataframe, "pydantic")
    assert getattr(dataframe.pydantic, "validate")
    assert getattr(dataframe.pydantic, "filter")


def test_validate(dataframe: pd.DataFrame):
    import pandantic.plugins.pandas

    assert dataframe.pydantic.validate(schema=DataFrameSchema1)
    assert not dataframe.pydantic.validate(schema=DataFrameSchema2)


def test_filter(dataframe: pd.DataFrame):
    import pandantic.plugins.pandas

    assert dataframe.pydantic.filter(schema=DataFrameSchema1).shape[0] == 3
    assert dataframe.pydantic.filter(schema=DataFrameSchema2).shape[0] == 0
