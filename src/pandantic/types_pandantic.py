import math
from typing import Annotated, Any, Optional, TypeVar

from pydantic.functional_validators import BeforeValidator


TypeT = TypeVar("TypeT")


def coerce_nan_to_none(x: Any) -> Any:
    """Coerce NaN values to None.

    Args:
        x (Any): The value to coerce.

    Returns:
        Any: The coerced value.
    """
    if x is None:
        return None

    if math.isnan(x):
        return None

    return x


Optional = Annotated[Optional[TypeT], BeforeValidator(coerce_nan_to_none)]
