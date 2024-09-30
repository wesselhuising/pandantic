"""A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

from __future__ import annotations

from typing import Any

import pandas as pd

from pandantic.types import DataFrameTypes, SchemaTypes
from pandantic.validators.baseclass import BaseValidator
from pandantic.validators.pandas import PandasValidator


class CoreValidator:
    """An implementation of the Pydantic BaseValidator."""

    def __init__(self, schema: SchemaTypes):
        self.schema = schema

    def _get_implementation(self, dataframe: DataFrameTypes) -> BaseValidator:
        if issubclass(pd.DataFrame, type(dataframe)):
            return PandasValidator(schema=self.schema)

        raise TypeError(f"Could not find any implementation for dataframe type: {type(dataframe)}")

    def validate(self, dataframe: DataFrameTypes, **args) -> Any:  # type: ignore
        return self._get_implementation(dataframe).validate(dataframe=dataframe, **args)
