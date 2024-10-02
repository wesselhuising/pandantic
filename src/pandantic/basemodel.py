"""A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

from __future__ import annotations

from typing import Any

import pandas as pd

from pandantic.types import SchemaTypes, TableTypes
from pandantic.validators.baseclass import BaseValidator
from pandantic.validators.pandas import PandasValidator


class CoreValidator:
    """A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

    def __init__(self, schema: SchemaTypes):
        self.schema = schema

    def _get_implementation(self, dataframe: TableTypes) -> BaseValidator:
        if issubclass(pd.DataFrame, type(dataframe)):
            return PandasValidator(schema=self.schema)

        raise TypeError(
            f"Could not find any implementation for dataframe type: {type(dataframe)}"
        )

    def validate(self, dataframe: TableTypes, **args) -> Any:  # type: ignore
        return self._get_implementation(dataframe).validate(dataframe=dataframe, **args)
