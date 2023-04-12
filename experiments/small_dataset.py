"""Test performance of pandantic on a small dataset."""

import logging
from pathlib import Path
from timeit import timeit

import pandas as pd

from pandantic import BaseModel  # type: ignore

logging.basicConfig(level=logging.INFO)

logging.info("loading dataset.")
df_test = pd.read_csv(
    filepath_or_buffer=Path(__file__).parent / "artefacts" / "test.csv",
)

logging.info("shape of df_test: %s", df_test.shape)


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    key: str
    pickup_datetime: str
    pickup_longitude: float
    pickup_latitude: float
    dropoff_longitude: float
    dropoff_latitude: float
    passenger_count: int


logging.info("starting validation.")
print(
    timeit(
        lambda: DataFrameSchema.parse_df(dataframe=df_test, errors="filter"),
        number=1000,
    )
    / 1000
)
print(
    timeit(
        lambda: DataFrameSchema.parse_df(dataframe=df_test, errors="filter"),
        number=1000,
    )
    / 1000
)
logging.info("finished validation.")

# logging.info("shape of df_valid: %s", df_valid.shape)
