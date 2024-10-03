from typing import Union

import pandas as pd
import pydantic


SchemaTypes = Union[pydantic.BaseModel]
TableTypes = Union[pd.DataFrame]
