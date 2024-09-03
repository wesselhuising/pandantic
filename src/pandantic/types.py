from typing import Union

import pandas as pd
import pydantic


SchemaTypes = Union[pydantic.BaseModel]
DataFrameTypes = Union[pd.DataFrame]
