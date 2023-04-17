import datetime

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
