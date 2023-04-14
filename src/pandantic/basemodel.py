"""A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pydantic import BaseModel
from pydantic.types import StrictInt


class PandanticBaseModel(BaseModel):
    """A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

    @classmethod
    def parse_df(
        cls,
        dataframe: pd.DataFrame,
        errors: str = "raise",
        context: dict[str, Any] | None = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Validate a DataFrame using the schema defined in the Pydantic model.

        Args:
            dataframe (pd.DataFrame): The DataFrame to validate.
            errors (str, optional): How to handle validation errors. Defaults to "raise".
            context (Optional[dict[str, Any], None], optional): The context to use for validation.
            verbose (bool, optional): Whether to log validation errors. Defaults to True.

        Returns:
            pd.DataFrame: The DataFrame with valid rows in case of errors="filter".
        """
        error_logs = {}
        for index, row in enumerate(dataframe.to_dict("records")):
            try:
                cls.model_validate(
                    obj=row,
                    context=context,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                if verbose:
                    logging.info("Validation error found at index %s\n%s", index, exc)
                error_logs[index] = exc

        if len(error_logs) > 0 and errors == "raise":
            raise ValueError(f"{len(error_logs)} validation errors found in dataframe.")
        if len(error_logs) > 0 and errors == "filter":
            return dataframe.drop(index=list(error_logs.keys()))

        return dataframe


class DataFrameSchema(PandanticBaseModel):
    """Example schema for testing."""

    example_str: str
    example_int: StrictInt


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("debug-logger")
    logger.debug("Executing basemodel.py from pydfntic package.")

    logger.debug("Type of DataFrameSchema: %s", type(DataFrameSchema))

    example_df_valid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", "1"],
            "example_int": [1, 2, 3],
        }
    )

    example_df_invalid = pd.DataFrame(
        data={
            "example_str": ["foo", "bar", 1],
            "example_int": ["1", 2, 3.0],
        }
    )

    df_valid = DataFrameSchema.parse_df(example_df_valid)

    assert df_valid.equals(example_df_valid)

    df_invalid = DataFrameSchema.parse_df(example_df_invalid, errors="filter")
    logging.info(df_invalid)
