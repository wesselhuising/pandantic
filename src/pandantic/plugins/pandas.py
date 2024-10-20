"""Pandas DataArray 'accessor' plugin.
See: https://pandas.pydata.org/pandas-docs/version/2.1/development/extending.html

Adds the following methods:
    df.pydantic.validate()
    df.pydantic.filter()
    df.pydantic.itertuples()
    df.pydantic.iterrows()
    df.pydantic.iterschemas()

To register plugin: `from pandantic.plugins import pandas_plugin #(or *)`
"""
import logging
from collections.abc import Hashable, Iterable
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, ValidationError

from pandantic.basemodel import CoreValidator


logger = logging.getLogger(__name__)


@pd.api.extensions.register_dataframe_accessor("pandantic")
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
        **kwargs: Optional[dict[str, Any]],
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
            )
        except Exception as e:
            logger.info(f"Invalid dataframe for {schema=}. Exception: {e}")
            return False
        logger.info(f"Valid dataframe for {schema=}!")
        return True

    def filter(
        self,
        schema: BaseModel,
        n_jobs: Optional[int] = None,
        verbose: bool = True,
        **kwargs: Optional[dict[str, Any]],
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

    def itertuples(
        self,
        schema: BaseModel,
        verbose: bool = True,
    ) -> Iterable[tuple[Any, ...]]:
        """Same as normal .itertuples(), except invalid rows are skipped."""
        for row in self.obj.itertuples(name=None):
            try:
                _ = schema(**dict(zip(self.obj.columns, row[1:])))  # type: ignore
            except ValidationError as e:
                if verbose:
                    logger.info(f"Invalid row {row} with error: {e}")
                continue
            yield row

    def iterrows(  # type: ignore[no-untyped-def]
        self, schema: BaseModel, verbose: bool = True, **kwargs
    ) -> Iterable[tuple[Hashable, pd.Series]]:  # type: ignore[type-arg]
        """Same as normal .iterrows(), except invalid rows are skipped."""
        schema_validator = CoreValidator(schema)
        for i, _ in schema_validator.iterate(dataframe=self.obj, context=kwargs, verbose=verbose):
            yield i, self.obj.loc[i]  # type: ignore[call-overload]

    def iterschemas(  # type: ignore[no-untyped-def]
        self, schema: BaseModel, verbose: bool = True, **kwargs
    ) -> Iterable[tuple[Hashable, Any]]:
        """Iterate over DataFrame rows as validated schema models."""
        schema_validator = CoreValidator(schema)
        return schema_validator.iterate(dataframe=self.obj, context=kwargs, verbose=verbose)
