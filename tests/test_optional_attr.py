import pandas as pd
import pytest
from pydantic import ValidationError

from pandantic import BaseModel


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

        class Model(BaseModel):
            a: Optional[int] = None

        df_example = pd.DataFrame({"a": [1, None, 2]})

        m = Model.parse_df(pd.DataFrame({"a": [1, None, 2]}), errors="filter", verbose=True)
        assert m.equals(df_example)

    def test_optional_int_parse_df_all_none(self):
        from pandantic import Optional  # pylint: disable=import-outside-toplevel

        # GIVEN
        class Model(BaseModel):
            a: Optional[int] = None

        df_example = pd.DataFrame({"a": [None, None, None]})

        # WHEN
        m = Model.parse_df(pd.DataFrame({"a": [None, None, None]}), errors="filter", verbose=True)

        # THEN
        assert m.equals(df_example)
