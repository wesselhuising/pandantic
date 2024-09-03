"""A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

from __future__ import annotations

import pandas as pd

from pandantic.types import DataFrameTypes, SchemaTypes
from pandantic.validators import PandasValidator


class CoreValidator:
    """A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

    def __init__(self, schema: SchemaTypes):
        self.schema = schema

    def _get_implementation(self, dataframe: DataFrameTypes):
        if issubclass(pd.DataFrame, type(dataframe)):
            return PandasValidator(schema=self.schema)

        raise TypeError("Could not find any implementation for dataframe type: %s", type(dataframe))

    def validate(self, dataframe: DataFrameTypes, **args):
        return self._get_implementation(dataframe).validate(dataframe=dataframe, **args)
