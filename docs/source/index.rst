Welcome to Pandantic's documentation!
=====================================

Gone are the days of black-box dataframes in otherwise type-safe code!

Pandantic builds off the Pydantic API to enable validation and filtering of the usual dataframe types (i.e., pandas, etc.)

.. _installation:

Installation
------------

Install Pandantic using pip:

.. code-block:: console

   $ pip install pandantic

Quick Start
----------

Here's a simple example demonstrating how to validate a pandas DataFrame:

.. code-block:: python

    import pandas as pd
    from pydantic import BaseModel

    from pandantic import Pandantic

    # Define your schema using Pydantic
    class EmployeeSchema(BaseModel):
        name: str
        salary: int
        department: str

    # Create sample DataFrame with mixed valid/invalid data
    df = pd.DataFrame({
        "name": ["Alice Smith", "Bob Jones", 123],          # Last row: invalid name
        "salary": [50000, "high", 60000],                  # Second row: invalid salary
        "department": ["Engineering", "Sales", "Marketing"]
    })

    # Initialize validator
    validator = Pandantic(schema=EmployeeSchema)

    # Method 1: Skip invalid rows
    df_valid = validator.validate(dataframe=df, errors="skip")
    print(f"Valid rows: {len(df_valid)} out of {len(df)}")

    # Method 2: Raise error on invalid data
    try:
        validator.validate(dataframe=df, errors="raise")
    except ValueError as e:
        print(f"Validation error: {e}")

Key Features
-----------
- Validate or filter DataFrame rows using Pydantic models.
- Different "`errors`" modes to control invalid data handling.
- Full compatibility with Pydantic's type system and validators.
- Simple, intuitive API following pandas conventions.
- Alternatively provides a `pandas` plugin API.


Contents
--------

.. toctree::

   usage
   pandas_plugin
   api
