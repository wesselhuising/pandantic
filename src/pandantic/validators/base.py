from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable
from typing import Any


class BaseValidator(ABC):
    @abstractmethod
    def validate(self, dataframe: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def iterate(self, dataframe: Any) -> Iterable[tuple[Hashable, Any]]:
        """Iterates over the rows and generate only validated schema models.

        NOTE: This is similar to iterrows() in pandas, except non-valid rows are
            skipped.
        """
        raise NotImplementedError
