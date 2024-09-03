from abc import ABC, abstractmethod
from typing import Any


class BaseValidator(ABC):
    @abstractmethod
    def validate(self, dataframe: Any) -> Any:
        pass
