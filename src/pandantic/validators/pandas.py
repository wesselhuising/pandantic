import logging
import math
import os
from collections.abc import Hashable, Iterable
from typing import Any, Literal, Optional

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
        errors: Literal["skip", "raise", "log"] = "raise",
        strict: bool = False,
        context: Optional[
            dict[str, Any]
        ] = None,  # pylint: disable=consider-alternative-union-syntax,useless-suppression
        n_jobs: int = 1,
        queue: Optional[Queue] = None,
    ) -> pd.DataFrame:
        """Validate a DataFrame using the schema defined in the Pydantic model.

        Args:
            dataframe (pd.DataFrame): The DataFrame to validate.
            errors (Literal["skip", "raise", "log"], optional): How to handle validation errors. Defaults to "raise".
                NOTE: "skip" and "log" effectively filter the dataframe, excluding invalid rows.
            strict (bool, default=False): whether to fail validation if extra fields/columns are present.
            context (Optional[dict[str, Any]], optional): The context to use for validation. Defaults to None.
            n_jobs (int, optional): The number of processes to use for validation. Defaults to 1.
            queue (Optional[Queue], optional): A custom Queue object for multiprocessing. Defaults to None.

        Returns:
            pd.DataFrame: The original DataFrame if errors="raise" or "log", or a filtered DataFrame with valid rows if errors="skip".
        """
        # check for extra columns and handle strict mode
        # NOTE: this will need to be abstracted to handle different types of schema objects
        if strict:
            extras = {
                col for col in dataframe.columns if col not in self.schema.model_fields.keys()
            }
            if extras:
                raise ValueError(
                    f"Strict mode is enabled but the following extra columns were found in the schema: {extras}."
                )
            del extras

        if errors not in ["skip", "raise", "log"]:
            raise ValueError("errors must be one of 'skip', 'raise', or 'log'")

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
                p = Process(
                    target=self._validate_chunk,
                    args=(chunks[i], errors, queue),
                    daemon=True,
                )
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
                    if errors == "log":
                        logging.info("Validation error found at index %s\n%s", index, exc)
                    if errors == "raise":
                        raise exc
                    errors_index.append(index)

        logging.debug("# invalid rows: %s", len(errors_index))

        if len(errors_index) > 0 and errors in ["skip", "log"]:
            return dataframe[~dataframe.index.isin(list(errors_index))]
        return dataframe

    def _validate_chunk(
        self,
        chunk: dict[Hashable, Any],
        queue: Any,
        errors: Literal["skip", "raise", "log"] = "raise",
        context: Optional[
            dict[str, Any]
        ] = None,  # pylint: disable=consider-alternative-union-syntax,useless-suppression
    ) -> None:
        """Validate a single chunk of a DataFrame converted to a dictionary w/ index values as keys.

        Args:
            chunk (dict[Hashable, Any]): The DataFrame chunk to validate.
            errors (Literal["skip", "raise", "log"], optional): How to handle validation errors. Defaults to "raise".
                NOTE: "skip" and "log" effectively filter the dataframe, excluding invalid rows.
            queue (Optional[Queue], optional): The queue to put the index of the row in case of an error.
            context (Optional[dict[str, Any]], optional): The context to use for validation. Defaults to None.
        """
        logging.debug("Process started.")

        for index, row_dict in chunk.items():
            try:
                self.schema.model_validate(
                    obj=row_dict,
                    context=context,
                )
            except ValidationError as exc:  # pylint: disable=broad-exception-caught
                if errors == "log":
                    logging.info("Validation error found at index %s\n%s", index, exc)
                if errors == "raise":
                    raise exc
                queue.put(index)

        logging.debug("Process ended.")

        queue.put(None)

    def iterate(
        self,
        dataframe: pd.DataFrame,
        context: Optional[
            dict[str, Any]
        ] = None,  # pylint: disable=consider-alternative-union-syntax,useless-suppression
        verbose: bool = True,
    ) -> Iterable[tuple[Hashable, SchemaTypes]]:
        """Iterate over a DataFrame and yield validated schema models."""
        for i, row in dataframe.iterrows():
            try:
                yield (
                    i,
                    self.schema.model_validate(
                        obj=row.to_dict(),
                        context=context,
                    ),
                )
            except Exception as e:
                if verbose:
                    logging.info(f"Validation error found at index {i}, skipping: {e}.")
                continue
