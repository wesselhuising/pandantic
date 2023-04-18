Usage
=====

.. _installation:

Installation
------------

To use Pandatic, first install it using pip:

.. code-block:: console

   (.venv) $ pip install pandantic

Validating dataframes using Pydantic API
----------------------------------------

To validate ``pd.DataFrames`` using ``pydantic.BaseModel``s make sure to import
the ``BaseModel`` class from the ``pandantic`` package.

.. code-block:: python

    from pydantic.types import StrictInt

    from pandantic import BaseModel


    class DataFrameSchema(BaseModel):
        """Example schema for testing."""

        example_str: str
        example_int: StrictInt

Let's try this schema on a simple ``pandas.DataFrame``. Use the class method
```parse_df``` from the freshly defined ```DataFrameSchema``` and specify the
```dataframe``` that should be validated using the arguments of the method.

.. code-block:: python

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

.. autofunction:: pandantic.BaseModel.parse_df
