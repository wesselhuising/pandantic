"""Pandas DataArray 'accessor' plugin.
See: https://pandas.pydata.org/pandas-docs/version/2.1/development/extending.html

Adds the following methods:
    df.pydantic.validate()
    df.pydantic.filter()
    df.pydantic.to_records()
    df.pydantic.iterrows()
    df.pydantic.iter_tuples()
"""
import logging
import pandas as pd
from typing import Optional
from pydantic import BaseModel
from pandantic import PandanticBaseModel

# TODO: figure out an elegent way to work with dataclasses, which are weakly typed


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

        def validate(self, schema: BaseModel):
            ...

        def validate_deprecated(
            self,
            schema: PandanticBaseModel,
            n_jobs: Optional[int],
            verbose: bool = True,
            **kwargs,
        ) -> bool:
            assert isinstance(schema, PandanticBaseModel)
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

        def filter_deprecated(
            self,
            schema: PandanticBaseModel,
            n_jobs: Optional[int],
            verbose: bool = True,
            **kwargs,
        ):
            assert isinstance(schema, PandanticBaseModel)
            return schema.parse_df(
                dataframe=self.obj,
                errors="filter",
                context=kwargs,
                n_jobs=n_jobs or 1,
                verbose=verbose,
            )
