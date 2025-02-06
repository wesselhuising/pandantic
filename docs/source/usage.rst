Pandantic usage
===============

Installation
------------

First, install ``pandantic`` by using pip (or any other package managing tool).

.. code-block:: bash

    pip install pandantic

Using the validator
-------------------

.. code-block:: python
  from pydantic import BaseModel
  from pydantic.types import StrictInt

  from pandantic import Pandantic

  # Define your schema using Pydantic BaseModel
  class DataFrameSchema(BaseModel):
      """Example schema for testing."""
      example_str: str
      example_int: StrictInt

  # Create a validator instance
  validator = Pandantic(schema=DataFrameSchema)

  # Example DataFrame with some invalid data
  df_invalid = pd.DataFrame(
      data={
          "example_str": ["foo", "bar", 1],  # Last value is invalid (int instead of str)
          "example_int": ["1", 2, 3.0],      # First and last values are invalid (str and float)
      }
  )

  # Validate with error raising
  try:
      validator.validate(dataframe=df_invalid, errors="raise")
  except ValueError:
      print("Validation failed!")

  # Or filter out invalid rows
  df_valid = validator.validate(dataframe=df_invalid, errors="skip")
  # Only the second row remains as it's the only valid one

The validator supports two modes:

- ``errors="raise"``: Raises a ValueError if any row fails validation
- ``errors="skip"``: Returns a new DataFrame with only the valid rows


Advanced features
=================

Strict Type Validation
----------------------

The validator supports Pydantic's strict types for more rigorous validation:

.. code-block:: python

  from pydantic import BaseModel
  from pydantic.types import StrictInt
  from pandantic import Pandantic

  class StrictSchema(BaseModel):
      example_str: str
      example_int: StrictInt  # Will only accept actual integers

  validator = Pandantic(schema=StrictSchema)
  df = pd.DataFrame({
      "example_str": ["foo", "bar"],
      "example_int": [1, "2"]  # Second value will fail as it's a string
  })

  # This will only keep the first row
  df_valid = validator.validate(dataframe=df, errors="skip")

Custom Validators
-----------------

You can still use all of Pydantic's validation features in your schema:

.. code-block:: python

  from pydantic import BaseModel, field_validator
  from pandantic import Pandantic

  class CustomSchema(BaseModel):
      example_str: str
      example_int: int

      @field_validator("example_int")
      def must_be_even(cls, v: int) -> int:
          if v % 2 != 0:
              raise ValueError("Number must be even")
          return v

  validator = Pandantic(schema=CustomSchema)

Optional Fields
---------------

As the DataFrame is being parsed into a dict, a ``None`` value is considered as a ``nan`` value in cases there are different values in the dict. Therefore, specifying ``Optional`` columns (where the value can be empty) can be specified by using the custom ``pandantic.Optional`` type. This type is a replacement for ``typing.Optional``.

.. code-block:: python

  from pydantic import BaseModel
  from pandantic import Optional  # pylint: disable=import-outside-toplevel

  # GIVEN
  class Model(BaseModel):
      a: Optional[int] = None
      b: int

  df_example = pd.DataFrame({"a": [1, None, 2], "b": ["str", 2, 3]})

  validator = Pandantic(schema=Model)
