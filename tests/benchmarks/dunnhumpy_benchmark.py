from pathlib import Path
from timeit import timeit

import pandas as pd
import pandera as pa
from models import dunnhumby  # pylint: disable=import-error


def pandera_validate(input_df):
    df_filtered = None
    try:
        schema.validate(input_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        df_filtered = input_df[~input_df.index.isin(exc.failure_cases["index"])]

    return df_filtered


if __name__ == "__main__":
    df = pd.read_csv(
        filepath_or_buffer=Path(__file__).parent.parent.parent / "artefacts" / "dunnhumby.csv",
        sep=",",
    )

    print(df.shape)
    print(df.head())

    schema = pa.DataFrameSchema(
        {
            "household_key": pa.Column(pa.Int),
            "BASKET_ID": pa.Column(pa.Int),
            "DAY": pa.Column(pa.Int),
            "PRODUCT_ID": pa.Column(pa.Int),
            "QUANTITY": pa.Column(
                pa.Int,
                [pa.Check(lambda x: x <= 5), pa.Check.greater_than_or_equal_to(0)],
            ),
            "SALES_VALUE": pa.Column(pa.Float),
            "STORE_ID": pa.Column(pa.Int),
            "RETAIL_DISC": pa.Column(pa.Float, pa.Check.less_than_or_equal_to(0)),
            "TRANS_TIME": pa.Column(pa.Int),
            "COUPON_DISC": pa.Column(pa.Float, pa.Check.less_than_or_equal_to(0)),
            "COUPON_MATCH_DISC": pa.Column(pa.Float, pa.Check.less_than_or_equal_to(0)),
        },
        strict=True,
    )

    df_valid_pandantic = dunnhumby.DunnhumbySchema.parse_df(
        dataframe=df,
        errors="filter",
        n_jobs=4,
    )
    print(f"shape of df_valid_pandantic: {df_valid_pandantic.shape}")

    df_valid_pandera = pandera_validate(df)
    print(f"shape of df_valid_pandera:   {df_valid_pandera.shape}")

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

    print(
        "pandantic is",
        timeit(
            lambda: dunnhumby.DunnhumbySchema.parse_df(
                dataframe=df,
                errors="filter",
                n_jobs=1,
            ),
            number=10,
        )
        / 10,
    )

    print(
        "pandera is  ",
        timeit(
            lambda: pandera_validate(df),
            number=10,
        )
        / 10,
    )
