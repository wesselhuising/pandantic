import pandas as pd
import pytest
from pydantic import BaseModel, ValidationError

from pandantic import Pandantic


class TestOptional:
    def test_optional_int_basemodel_with_default(self):
        """Test that an optional int with a default value is set to None when not provided."""
        from typing import Optional  # pylint: disable=import-outside-toplevel

        # GIVEN
        class Model(BaseModel):
            a: Optional[int] = None  # pylint: disable=consider-alternative-union-syntax

        # WHEN
        m_int = Model(a=1)
        m_none = Model(a=None)
        m = Model()

        # THEN
        assert m_int.a == 1
        assert m_none.a is None
        assert m.a is None

    def test_optional_int_basemodel_without_default(self):
        """Test that an optional int without a default value is set to None when not provided."""
        from typing import Optional  # pylint: disable=import-outside-toplevel

        # GIVEN
        class Model(BaseModel):
            a: Optional[int]  # pylint: disable=consider-alternative-union-syntax

        # WHEN
        m_int = Model(a=1)
        m_none = Model(a=None)

        # THEN
        assert m_int.a == 1
        assert m_none.a is None

        with pytest.raises(ValidationError):
            m = Model()  # pylint: disable=unused-variable

    def test_optional_int_parse_df_with_default(self):
        """Test that an optional int with a default value is set to None when not provided."""
        from pandantic import Optional  # pylint: disable=import-outside-toplevel

        # GIVEN
        class Model(BaseModel):
            a: Optional[int] = None
            b: int

        df_example = pd.DataFrame({"a": [1, None, 2], "b": ["str", 2, 3]})

        validator = Pandantic(schema=Model)

        # WHEN
        df_filtered = validator.validate(df_example, errors="filter", verbose=True)

        # THEN
        assert df_filtered.equals(df_example.drop(index=[0]))

    def test_optional_int_parse_df_all_none(self):
        from pandantic import Optional  # pylint: disable=import-outside-toplevel

        # GIVEN
        class Model(BaseModel):
            a: Optional[int] = None
            b: str

        df_example = pd.DataFrame({"a": [None, None, None], "b": ["str", "str", "str"]})

        validator = Pandantic(schema=Model)

        # WHEN
        df_filtered = validator.validate(df_example, errors="filter", verbose=True)

        # THEN
        assert df_filtered.equals(df_example)
