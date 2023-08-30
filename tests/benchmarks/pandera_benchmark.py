from pathlib import Path
from timeit import timeit

import pandas as pd
import pandera as pa

from tests.benchmarks.models import dataframeschema  # pylint: disable=import-error


def pandera_validate(input_df):
    df_filtered = None
    try:
        schema.validate(input_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        df_filtered = input_df[~input_df.index.isin(exc.failure_cases["index"])]

    return df_filtered


if __name__ == "__main__":
    df = pd.read_csv(
        filepath_or_buffer=Path(__file__).parent.parent.parent / "artefacts" / "nfl.csv",
    )[
        [
            "Date",
            "GameID",
            "down",
            "time",
            "yrdline100",
            "SideofField",
            "DefensiveTeam",
            "PosTeamScore",
            "DefTeamScore",
            "PlayType",
        ]
    ]  # .loc[:1000]

    print(df.shape)
    print(df.head())

    schema = pa.DataFrameSchema(
        {
            "household_key": pa.Column(pa.Int),
            "BASKET_ID": pa.Column(pa.Int),
            "DAY": pa.Column(pa.Int),
            "PRODUCT_ID": pa.Column(pa.Int),
            "QUANTITY": pa.Column(pa.UInt, pa.Check(lambda x: x <= 5)),
            "SALES_VALUE": pa.Column(pa.Float),
            "STORE_ID": pa.Column(pa.Int),
            "RETAIL_DISC": pa.Column(pa.Float, pa.Check.lower_than_or_equal_to(0)),
            "TRANS_TIME": pa.Column(pa.Int),
            "COUPON_DISC": pa.Column(
                pa.String,
                pa.Column(pa.Float, pa.Check.lower_than_or_equal_to(0)),
            ),
            "COUPON_MATCH_DISC": pa.Column(
                pa.String,
                pa.Column(pa.Float, pa.Check.lower_than_or_equal_to(0)),
            ),
        },
        strict=True,
    )

    df_valid_pandantic = dataframeschema.DataFrameSchema.parse_df(dataframe=df, errors="filter")
    print("shape of df_valid_pandantic: %s", df_valid_pandantic.shape)

    df_valid_pandera = pandera_validate(df)
    print("shape of df_valid_pandera: %s", df_valid_pandera.shape)

    assert (
        len(
            (
                df_valid_pandantic.merge(df_valid_pandera, how="outer", indicator=True).loc[
                    lambda x: x["_merge"] != "both"
                ]
            )
        )
        == 0
    )
    print(df.PlayType.unique())

    print(
        "pandantic is",
        timeit(
            lambda: dataframeschema.DataFrameSchema.parse_df(dataframe=df, errors="filter"),
            number=100,
        )
        / 100,
    )

    print(
        "pandera is  ",
        timeit(
            lambda: pandera_validate(df),
            number=100,
        )
        / 100,
    )
