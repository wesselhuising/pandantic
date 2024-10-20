"""Pandas DataArray 'accessor' plugin.
See: https://pandas.pydata.org/pandas-docs/version/2.1/development/extending.html

Adds the following methods:
    df.pydantic.validate()
    df.pydantic.filter()

To register plugin: `from pandantic.plugins import pandas_plugin #(or *)`
"""
import logging
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel

from pandantic.basemodel import CoreValidator


@pd.api.extensions.register_dataframe_accessor("pydantic")
class PydanticAccessor:
    def __init__(self, pandas_obj: pd.DataFrame):
        assert isinstance(pandas_obj, pd.DataFrame), "Only works with DataFrames!"
        if not any(isinstance(col, str) for col in pandas_obj.columns):
            raise AttributeError("Must have at least one string column name!")
        self._obj = pandas_obj

    @property
    def obj(self) -> pd.DataFrame:
        return self._obj

    def validate(
        self,
        schema: BaseModel,
        n_jobs: Optional[int] = None,
        verbose: bool = True,
        **kwargs: Optional[Dict[str, Any]],
    ) -> bool:
        if not isinstance(schema, type(BaseModel)):
            raise TypeError("Arg `schema` must be a pydantic.BaseModel subclass!")

        schema_validator = CoreValidator(schema)  # type: ignore
        try:
            _ = schema_validator.validate(
                dataframe=self.obj,
                errors="raise",
                context=kwargs,
                n_jobs=n_jobs or 1,
                verbose=verbose,
            )
        except Exception as e:
            logging.info(f"Invalid dataframe for {schema=}. Exception: {e}")
            return False
        logging.info(f"Valid dataframe for {schema=}!")
        return True

    def filter(
        self,
        schema: BaseModel,
        n_jobs: Optional[int] = None,
        verbose: bool = True,
        **kwargs: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        if not isinstance(schema, type(BaseModel)):
            raise TypeError("Arg `schema` must be a pydantic.BaseModel subclass!")

        schema_validator = CoreValidator(schema)  # type: ignore
        if verbose:
            errors = "log"
        else:
            errors = "skip"
        filtered_df: pd.DataFrame = schema_validator.validate(
            dataframe=self.obj,
            errors=errors,
            context=kwargs,
            n_jobs=n_jobs or 1,
        )
        assert isinstance(filtered_df, pd.DataFrame)
        return filtered_df
