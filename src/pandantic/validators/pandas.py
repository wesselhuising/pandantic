import logging
import math
import os
from collections.abc import Hashable
from typing import Any, Optional

import pandas as pd
from multiprocess import (  # type:ignore # pylint: disable=no-name-in-module
    Process,
    Queue,
)
from pydantic import ValidationError

from pandantic.types import SchemaTypes
from pandantic.validators.base import BaseValidator


class PandasValidator(BaseValidator):
    def __init__(self, schema: SchemaTypes):
        self.schema = schema

    def validate(
        self,
        dataframe: pd.DataFrame,
        errors: str = "raise",
        context: Optional[
            dict[str, Any]
        ] = None,  # pylint: disable=consider-alternative-union-syntax,useless-suppression
        n_jobs: int = 1,
        queue: Optional[Queue] = None,
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

        if n_jobs < 1:
            if n_jobs < 0:
                n_jobs = os.cpu_count()  # type: ignore

            chunks = []
            chunk_size = math.floor(len(dataframe) / n_jobs)
            num_chunks = len(dataframe) // chunk_size + 1

            # handle user input for queue
            if queue is None:
                queue = Queue()
            elif "queue" not in str(type(queue)).lower():
                logging.warning(f"Expecting queue object for arg:queue, not {type(queue)}!")
            else:
                assert hasattr(queue, "get"), "Queue object must have a put method."
                assert hasattr(queue, "put"), "Queue object must have a put method."

            # send chunks to be processed
            for i in range(num_chunks):
                chunks.append(
                    dataframe.iloc[i * chunk_size : (i + 1) * chunk_size].to_dict("index")
                )

            for i in range(num_chunks):
                p = Process(target=self._validate_chunk, args=(chunks[i], queue), daemon=True)
                p.start()

            num_stops = 0
            for i in range(num_chunks):
                while True:
                    index = queue.get()
                    if index is None:
                        num_stops += 1
                        break

                    errors_index.append(index)

                if num_stops == num_chunks:
                    break
        else:
            for index, row_dict in dataframe.to_dict("index").items():
                try:
                    self.schema.model_validate(
                        obj=row_dict,
                        context=context,
                    )
                except ValidationError as exc:  # pylint: disable=broad-exception-caught
                    if verbose:
                        print(exc)
                        logging.info("Validation error found at index %s\n%s", index, exc)

                    errors_index.append(index)

        logging.debug("# invalid rows: %s", len(errors_index))

        if len(errors_index) > 0 and errors == "raise":
            raise ValueError(f"{len(errors_index)} validation errors found in dataframe.")
        if len(errors_index) > 0 and errors == "filter":
            return dataframe[~dataframe.index.isin(list(errors_index))]

        return dataframe

    def _validate_chunk(
        self,
        chunk: dict[Hashable, Any],
        q: Queue,
        context: Optional[
            dict[str, Any]
        ] = None,  # pylint: disable=consider-alternative-union-syntax,useless-suppression
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

        for index, row_dict in chunk.items():
            try:
                self.schema.model_validate(
                    obj=row_dict,
                    context=context,
                )
            except ValidationError as exc:  # pylint: disable=broad-exception-caught
                if verbose:
                    logging.info("Validation error found at index %s\n%s", index, exc)

                q.put(index)

        logging.debug("Process ended.")
        q.put(None)
