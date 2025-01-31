Usage
=====

Below are some usage examples in more nuanced scenarios.

Custom validator example
------------------------

One of the great features of Pydantic is the ability to ## Custom validator example

One of the great features of Pydantic is the ability to create custom validators. Luckily, those custom validators will also work when parsing DataFrames using `pandantic`. Make sure to import the original decorator from the `pydantic` package and keep in mind that `pandantic` is using the V2 of Pydantic (so `field_validation` it is). In the example below the `BaseModel` will validate the `example_int` field and makes sure it is an even number.

from pydantic import ValidationError, field_validator


.. code-block:: python
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

By setting the `errors` argument to `raise`, the code will raise an ValueError after validating every row as the first row contains an uneven number.

.. code-block:: python
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

Special fields and types
------------------------

As the DataFrame is being parsed into a dict, a `None` value is considered as a `nan` value in cases there are different values in the dict. Therefore, specifying `Optional` columns (where the value can be empty) can be speciyfied by using the custom `pandantic.Optional` type. This type is a replacement for `typing.Optional`.

.. code-block:: python
    from pandantic import BaseModel, Optional

    class Model(BaseModel):
        a: Optional[int] = None
        b: int

    df_example = pd.DataFrame({"a": [1, None, 2], "b": ["str", 2, 3]})

    df_filtered = Model.parse_df(df_example, errors="filter", verbose=True)
