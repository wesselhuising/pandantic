import logging
import math
import os
from typing import Any, Optional

import pandas as pd
from multiprocess import (  # type:ignore # pylint: disable=no-name-in-module
    Process,
    Queue,
)

from pandantic.types import SchemaTypes
from pandantic.validators.baseclass import BaseValidator


class PandasValidator(BaseValidator):
    def __init__(self, schema: SchemaTypes):
        self.schema = schema

    def validate(
        self,
        dataframe: pd.DataFrame,
        errors: str = "raise",
        context: Optional[dict[str, Any]] = None,  # pylint: disable=consider-alternative-union-syntax
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

        dataframe = dataframe.copy()
        dataframe["_index"] = dataframe.index

        if n_jobs != 1:
            if n_jobs < 0:
                n_jobs = os.cpu_count()  # type: ignore

            chunks = []
            chunk_size = math.floor(len(dataframe) / n_jobs)
            num_chunks = len(dataframe) // chunk_size + 1

            q = Queue()

            for i in range(num_chunks):
                chunks.append(dataframe.iloc[i * chunk_size : (i + 1) * chunk_size])

            for i in range(num_chunks):
                p = Process(target=self._validate_row, args=(chunks[i], q), daemon=True)
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
            for row in dataframe.to_dict("records"):
                try:
                    self.schema.model_validate(
                        obj=row,
                        context=context,
                    )
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    if verbose:
                        print(exc)
                        logging.info(
                            "Validation error found at index %s\n%s", row["_index"], exc
                        )

                    errors_index.append(row["_index"])

        logging.debug("# invalid rows: %s", len(errors_index))

        if len(errors_index) > 0 and errors == "raise":
            raise ValueError(
                f"{len(errors_index)} validation errors found in dataframe."
            )
        if len(errors_index) > 0 and errors == "filter":
            return dataframe[~dataframe.index.isin(list(errors_index))].drop(
                columns=["_index"]
            )

        return dataframe.drop(columns=["_index"])

    def _validate_row(
        self,
        chunk: pd.DataFrame,
        q: Queue,
        context: Optional[dict[str, Any]] = None,  # pylint: disable=consider-alternative-union-syntax
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
                self.schema.model_validate(
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
