"""Tests that our core Pandantic class routes to implemenations correctly."""
import pandas as pd
import pytest
from pydantic import BaseModel

from pandantic import Pandantic
from pandantic.validators.pandas import PandasValidator


class PydanticBasdeModel(BaseModel):
    a: int
    b: str


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame(
        data={
            "a": [1, 2, 3],
            "b": ["foo", "bar", "baz"],
        }
    )


def test_get_pandas_implementation(df: pd.DataFrame):
    base_model_validator = Pandantic(PydanticBasdeModel)
    impl = base_model_validator._get_implementation(df)
    assert isinstance(impl, PandasValidator)
