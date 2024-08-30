"""Pandas DataArray 'accessor' plugin.
See: https://pandas.pydata.org/pandas-docs/version/2.1/development/extending.html

Adds the following methods:
    df.pydantic.validate()
    df.pydantic.filter()

To register plugin: `from pandantic.plugins import pandas_plugin #(or *)`
"""
import logging
from typing import Optional

import pandas as pd

from pandantic import BaseModel as PandanticBaseModel


@pd.api.extensions.register_dataframe_accessor("pydantic")
class PydanticAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    @property
    def obj(self):
        """Validate that we have at least one string column name."""
        assert isinstance(self._obj, pd.DataFrame)
        if not any(isinstance(col, str) for col in self._obj.columns):
            raise AttributeError("Must have at least one string column name!")
        return self._obj

    def validate(
        self,
        schema: PandanticBaseModel,
        n_jobs: Optional[int] = None,
        verbose: bool = True,
        **kwargs,
    ) -> bool:
        assert issubclass(schema, PandanticBaseModel), f"{schema=} is not a PandanticBaseModel!"
        try:
            _ = schema.parse_df(
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
        schema: PandanticBaseModel,
        n_jobs: Optional[int] = None,
        verbose: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        assert issubclass(schema, PandanticBaseModel), f"{schema=} is not a PandanticBaseModel!"
        return schema.parse_df(
            dataframe=self.obj,
            errors="filter",
            context=kwargs,
            n_jobs=n_jobs or 1,
            verbose=verbose,
        )
