import datetime
from pathlib import Path
from timeit import timeit

import pandas as pd
import pandera as pa
from pydantic import StrictFloat, ValidationError, confloat, field_validator

from pandantic import BaseModel

LIST_PLAY_TYPE = [
    "Kickoff",
    "Pass",
    "Run",
    "Punt",
    "Sack",
    # "Field Goal",
    "No Play",
    # "Quarter End",
    "Two Minute Warning",
    "Timeout",
    # "Extra Point",
    "QB Kneel",
    "End of Game",
]

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
]  # .loc[:10000]

print(df.shape)
print(df.head())


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    Date: datetime.date
    GameID: int
    down: StrictFloat
    time: str
    yrdline100: confloat(le=100, allow_inf_nan=False)
    SideofField: str
    DefensiveTeam: str
    PosTeamScore: confloat(ge=0, allow_inf_nan=False)
    DefTeamScore: confloat(ge=0, allow_inf_nan=False)
    PlayType: str

    @field_validator("PlayType")
    def validate_playtype(  # pylint: disable=invalid-name, no-self-argument
        cls, x: str
    ) -> float:
        """Example custom validator to validate if int is even."""
        if x not in LIST_PLAY_TYPE:
            raise ValidationError(f"playtype must be in {LIST_PLAY_TYPE}, is {x}.")
        return x


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
        "PlayType": pa.Column(pa.String, pa.Check.isin(LIST_PLAY_TYPE)),
    }
)

df_valid_pandantic = DataFrameSchema.parse_df(dataframe=df, errors="filter")
print("shape of df_valid_pandantic: %s", df_valid_pandantic.shape)


def pandera_validate(input_df):
    df_filtered = None
    try:
        schema.validate(input_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        df_filtered = input_df[~input_df.index.isin(exc.failure_cases["index"])]

    return df_filtered


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
        lambda: DataFrameSchema.parse_df(dataframe=df, errors="filter"),
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
