from pathlib import Path
from timeit import timeit

import pandas as pd
import pandera as pa
from models import dataframeschema  # pylint: disable=import-error


def pandera_validate(input_df):
    df_filtered = None
    try:
        schema.validate(input_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        df_filtered = input_df[~input_df.index.isin(exc.failure_cases["index"])]

    return df_filtered


if __name__ == "__main__":
    df = pd.read_csv(
        filepath_or_buffer=Path(__file__).parent.parent.parent
        / "artefacts"
        / "nfl.csv",
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
    ]  # .loc[:10000]

    print(df.shape)
    print(df.head())

    schema = pa.DataFrameSchema(
        {
            "Date": pa.Column(pa.DateTime),
            "GameID": pa.Column(pa.Int),
            "down": pa.Column(pa.Float, nullable=True),
            "time": pa.Column(pa.String),
            "yrdline100": pa.Column(pa.Float, pa.Check(lambda x: x <= 100)),
            "SideofField": pa.Column(pa.String),
            "DefensiveTeam": pa.Column(pa.String),
            "PosTeamScore": pa.Column(pa.Float, pa.Check.greater_than_or_equal_to(0)),
            "DefTeamScore": pa.Column(pa.Float, pa.Check.greater_than_or_equal_to(0)),
            "PlayType": pa.Column(
                pa.String, pa.Check.isin(dataframeschema.LIST_PLAY_TYPE)
            ),
        }
    )

    df_valid_pandantic = dataframeschema.DataFrameSchema.parse_df(
        dataframe=df,
        errors="filter",
        n_jobs=4,
    )
    print(f"shape of df_valid_pandantic: {df_valid_pandantic.shape}")

    df_valid_pandera = pandera_validate(df)
    print(f"shape of df_valid_pandera:   {df_valid_pandera.shape}")

    # print(df_valid_pandera[~df_valid_pandera.index.isin(df_valid_pandantic.index)].dropna())

    assert (
        len(
            (
                df_valid_pandantic.merge(
                    df_valid_pandera, how="outer", indicator=True
                ).loc[lambda x: x["_merge"] != "both"]
            )
        )
        == 0
    )

    print(
        "pandantic is",
        timeit(
            lambda: dataframeschema.DataFrameSchema.parse_df(
                dataframe=df,
                errors="filter",
                n_jobs=-1,
            ),
            number=5,
        )
        / 5,
    )

    print(
        "pandera is  ",
        timeit(
            lambda: pandera_validate(df),
            number=5,
        )
        / 5,
    )
