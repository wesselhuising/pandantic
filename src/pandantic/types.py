from typing import Union, TypeAlias

import pandas as pd
import pydantic


SchemaTypes: TypeAlias = Union[type[pydantic.BaseModel]]
TableTypes: TypeAlias = Union[pd.DataFrame]
