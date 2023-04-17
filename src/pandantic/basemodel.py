"""A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""
from __future__ import annotations

import logging
import math
import os
from typing import Any

import pandas as pd
from multiprocess import Process, Queue  # pylint: disable=no-name-in-module
from pydantic import BaseModel


class PandanticBaseModel(BaseModel):
    """A subclass of the Pydantic BaseModel that adds a parse_df method to validate DataFrames."""

    @classmethod
    def parse_df(
        cls,
        dataframe: pd.DataFrame,
        errors: str = "raise",
        context: dict[str, Any] | None = None,
        n_jobs: int = 1,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Validate a DataFrame using the schema defined in the Pydantic model.

        Args:
            dataframe (pd.DataFrame): The DataFrame to validate.
            errors (str, optional): How to handle validation errors. Defaults to "raise".
            context (Optional[dict[str, Any], None], optional): The context to use for validation.
            n_jobs (int, optional): The number of processes to use for validation. Defaults to 1.
            verbose (bool, optional): Whether to log validation errors. Defaults to True.

        Returns:
            pd.DataFrame: The DataFrame with valid rows in case of errors="filter".
        """
        errors_index = []
        logging.debug("Amount of available cores: %s", os.cpu_count())

        if n_jobs != 1:
            if n_jobs < 0:
                n_jobs = os.cpu_count()  # type: ignore

            chunks = []
            chunk_size = math.floor(len(dataframe) / n_jobs)
            num_chunks = len(dataframe) // chunk_size + 1

            q = Queue()

            dataframe["_index"] = dataframe.index
            for i in range(num_chunks):
                chunks.append(dataframe.iloc[i * chunk_size : (i + 1) * chunk_size])

            for i in range(num_chunks):
                p = Process(  # pylint: disable=not-callable
                    target=cls._validate_row, args=(chunks[i], q), daemon=True
                )
                p.start()

            num_stops = 0
            for i in range(num_chunks):
                while True:
                    index = q.get()
                    if index is None:
                        num_stops += 1
                        break

                    errors_index.append(index)

                if num_stops == num_chunks:
                    break
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

                    errors_index.append(index)

        if len(errors_index) > 0 and errors == "raise":
            raise ValueError(
                f"{len(errors_index)} validation errors found in dataframe."
            )
        if len(errors_index) > 0 and errors == "filter":
            return dataframe[~dataframe.index.isin(list(errors_index))]

        return dataframe

    @classmethod
    def _validate_row(
        cls,
        chunk: pd.DataFrame,
        q: Queue,
        context: dict[str, Any] | None = None,
        verbose: bool = True,
    ) -> None:
        """Validate a single row of a DataFrame.

        Args:
            chunk (pd.DataFrame): The DataFrame chunk to validate.
            q (Queue): The queue to put the index of the row in case of an error.
            context (Optional[dict[str, Any], None], optional): The context to use for validation.
            verbose (bool, optional): Whether to log validation errors. Defaults to True.
        """
        logging.debug("Process started.")

        for row in chunk.to_dict("records"):
            try:
                cls.model_validate(
                    obj=row,
                    context=context,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                if verbose:
                    logging.info(
                        "Validation error found at index %s\n%s", row["_index"], exc
                    )

                q.put(row["_index"])

        logging.debug("Process ended.")
        q.put(None)
