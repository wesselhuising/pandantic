# pandantic

`pandantic` introduces the ability to validate (`pandas`) DataFrames using `pydantic.BaseModel`s. The `pandantic` package is using the V2 version of `pydantic` as it has significant improvements over its V1 versions (a performance increase up to 50 times).

First, install `pandantic` by using pip (or any other package managing tool).

```pip install pandantic```

## parse_df

To validate `pd.DataFrame`s using Pydantic `BaseModel`s make sure to import the `BaseModel` class from the `pandantic` package.

```from pandantic import BaseModel```

The `pandantic.BaseModel` subclasses the original `pydantic.BaseModel` which means the `pandantic.BaseModel` includes all functionality from the original `pydantic.BaseModel` but it adds the `parse_df` class method which should be used to parse DataFrames.

## A quick example

Enough of the talking, lets just make things easier by showing a very minor but quick example. Make sure to import the `BaseModel` class from `pandantic` and create a schema like we normally would when using `pydantic`.

```
from pydantic.types import StrictInt

from pandantic import BaseModel


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    example_str: str
    example_int: StrictInt
```

Let's try this schema on a simple `pandas.DataFrame`. Use the class method `parse_df` from the freshly defined `DataFrameSchema` and specify the `dataframe` that should be validated using the arguments of the method. In this example, we want to `filter` out the bad records (there are more options like the good old `raise` to raise a ValueError after validating the whole DataFrame). In this case, only the second record would be kept in the returned DataFrame.

```
df_invalid = pd.DataFrame(
    data={
        "example_str": ["foo", "bar", 1],
        "example_int": ["1", 2, 3.0],
    }
)

df_filtered = DataFrameSchema.parse_df(
    dataframe=df_invalid,
    errors="filter",
)
```
### Custom validators

One of the great features of Pydantic is the ability to create custom validators. Luckily, those custom validators will also work when parsing DataFrames using `pandantic`. Make sure to import the original decorator from the `pydantic` package and keep in mind that `pandantic` is using the V2 of Pydantic (so `field_validation` it is). In the example below the `BaseModel` will validate the `example_int` field and makes sure it is an even number.

```
from pydantic import ValidationError, field_validator


class DataFrameSchema(BaseModel):
    """Example schema for testing."""

    example_str: str
    example_int: int

    @field_validator("example_int")
    def validate_even_integer(  # pylint: disable=invalid-name, no-self-argument
        cls, x: int
    ) -> int:
        """Example custom validator to validate if int is even."""
        if x % 2 != 0:
            raise ValidationError(f"example_int must be even, is {x}.")
        return x
```

By setting the `errors` argument to `raise`, the code will raise an ValueError after validating every row as the first row contains an uneven number.

```
example_df_invalid = pd.DataFrame(
    data={
        "example_str": ["foo", "bar", "baz"],
        "example_int": [1, 4, 12],
    }
)

df_raised_error = DataFrameSchema.parse_df(
    dataframe=example_df_invalid,
    errors="raise",
)
```
## Docs

Need to work on this, I know. I do wrote an [Medium blogpost](https://duckduckgo.com) about the usage and possibilites of the `pandantic` package (including some benchmarks).
