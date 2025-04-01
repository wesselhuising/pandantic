import logging
from enum import Enum

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from pandantic import Optional, Pandantic


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class PClass(int, Enum):
    FIRST = 1
    SECOND = 2
    THIRD = 3


class Sex(str, Enum):
    MALE = "male"
    FEMALE = "female"


class Embarked(str, Enum):
    CHERBOURG = "C"
    QUEENSTOWN = "Q"
    SOUTHAMPTON = "S"


class TitanicPassenger(BaseModel):
    survival: Optional[bool]
    pclass: PClass
    sex: Sex
    age: int = Field(ge=0, lt=100)  # greater than or equal to 0 and less than 100
    sibsp: int = Field(ge=0, lt=10)  # assuming max siblings/spouse is 9
    parch: int = Field(ge=0, lt=10)  # assuming max parents/children is 9
    ticket: str
    fare: float = Field(ge=0)  # fare cannot be negative
    cabin: Optional[str] = None
    embarked: Embarked


def main() -> None:
    validator = Pandantic(schema=TitanicPassenger)

    df = pd.read_csv("./titanic.csv", sep=",")
    logger.info("length before validation: %s", len(df))

    df_validated = validator.validate(df, errors="skip")
    logger.info("length after validation : %s", len(df_validated))

    # INFO: mimicing inference
    input_json = df.sample(1).iloc[0].to_dict()

    try:
        validated_obj = TitanicPassenger.model_validate(input_json)
    except ValidationError as exc:
        logger.error("validation error: %s", exc)


if __name__ == "__main__":
    main()
