"""A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""
from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
from multiprocess import Pool  # type: ignore
from pydantic import BaseModel


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
        errors_index = []
        logging.info("Amount of available cores: %s", os.cpu_count())
        n_jobs = 1
        if n_jobs != 1:
            with Pool(n_jobs) as pool:  # pylint: disable=not-callable
                res = pool.map(
                    cls._validate_row,
                    [
                        {
                            "index": index,
                            "row": row,
                            "context": context,
                            "verbose": verbose,
                        }
                        for index, row in enumerate(dataframe.to_dict("records"))
                    ],
                    chunksize=1000,
                )

            errors_index = [x for x in res if x is not None]
        else:
            for index, row in enumerate(dataframe.to_dict("records")):
                try:
                    cls.model_validate(
                        obj=row,
                        context=context,
                    )
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    if verbose:
                        logging.info(
                            "Validation error found at index %s\n%s", index, exc
                        )
                    # error_logs[index] = exc
                    errors_index.append(index)

        if len(errors_index) > 0 and errors == "raise":
            raise ValueError(
                f"{len(errors_index)} validation errors found in dataframe."
            )
        if len(errors_index) > 0 and errors == "filter":
            return dataframe[~dataframe.index.isin(list(errors_index))]

        return dataframe

    @classmethod
    def _validate_row(  # type: ignore
        cls,
        args,
    ) -> int | None:
        """Validate a single row of a DataFrame.

        Args:
            args (dict): The arguments to use for validation.

        Returns:
            int: The index of the row that failed validation.
        """
        try:
            cls.model_validate(
                obj=args["row"],
                context=args["context"],
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if args["verbose"]:
                logging.info(
                    "Validation error found at index %s\n%s", args["index"], exc
                )

            return int(args["index"])

        return None
