from abc import ABC, abstractclassmethod


class BaseValidator(ABC):
    @abstractclassmethod
    def validate(self):
        pass
