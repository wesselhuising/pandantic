Pandas plugin
=============

Another way to use `pandantic` is via our [`pandas.DataFrame` extension](https://pandas.pydata.org/docs/development/extending.html) plugin. This adds the following methods to `pandas` (once "registered" by `import pandantic.plugins.pandas`):
* `DataFrame.pandantic.validate(schema:PandanticBaseModel)`, which returns a boolean for all valid inputs.
* `DataFrame.pandantic.filter(schema:PandanticBaseModel)`, which wraps `PandanticBaseModel.parse_obj(errors="filter")` and returns as dataframe.
* `DataFrame.pandantic.iterschemas(schema:PandanticBaseModel)`, which wraps `PandanticBaseModel.parse_obj(errors="filter")` 
  which returns an iterable w/ row indices and the instantiated schema objects.

The plugin also supports existing `DataFrame` iteration methods, except that it only returns valid rows data:
* `DataFrame.pandantic.itertuples(schema:PandanticBaseModel)`, which wraps `PandanticBaseModel.parse_obj(errors="filter")` and returns as dataframe.
* `DataFrame.pandantic.iterrows(schema:PandanticBaseModel)`, which wraps `PandanticBaseModel.parse_obj(errors="filter")` and returns as dataframe.

.. code-block:: python
    from pandantic import BaseModel
    import pandantic.plugins.pandas

    # we start by defining a schema
    class MyModel(BaseModel):
        a: int
        b: str

    # next we create a valid DataFrame (df1) and a mixed DataFrame (df2)
    df1: pd.DataFrame = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
    df2: pd.DataFrame = pd.DataFrame({"a": [1, 2, "3"], "b": ["a", 3, "c"]})

    # now we can use the DataFrame extension methods to validate or filter
    df1.pandantic.validate(MyModel)  # returns True
    df1.pandantic.filter(MyModel)  # returns the same dataframe

    df2.pandantic.validate(MyModel)  # returns False
    df2.pandantic.filter(MyModel)  # returns the filtered DataFrame with only the first row

    # or we can use the different iteration methods
    for row in df1.pandantic.itertuples(MyModel):
        row: pd.NamedTuple

     for i, row in df1.pandantic.iterrows(MyModel):
         i: int
         row: pd.Series

    for idx, schema in df2.pandantic.iterschemas(MyModel):
        idx: Hashable
        schema: MyModel
