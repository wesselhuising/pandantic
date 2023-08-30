import timeit
import unittest

import numpy
import pandas


def iter_df_as_dicts(df):
    col_names = list(df.columns)
    return (dict(zip(col_names, row)) for row in df.itertuples(index=False, name=None))


class TestIterDf(unittest.TestCase):
    def get_source_data(self, n=3000000):
        return pandas.DataFrame(
            {
                "floats": numpy.random.random(size=n),
                "ints": numpy.random.randint(0, 1000, size=n),
                "strings": numpy.random.randint(0, 1000, size=n),
                "dates": numpy.random.randint(0, 1000, size=n),
            }
        )

    def _tolist_using_iter(self, source_data: pandas.DataFrame) -> list:
        return list(iter_df_as_dicts(source_data))

    def _tolist_using_to_dict(self, source_data: pandas.DataFrame) -> list:
        return source_data.to_dict(orient="records")

    def test_iteration(self):
        source_data = self.get_source_data()
        iter_dicts = self._tolist_using_iter(source_data)
        df_records = self._tolist_using_to_dict(source_data)

        self.assertEqual(iter_dicts, df_records)

    def test_performance(self):
        source_data = self.get_source_data(n=100000)

        iter_time = timeit.timeit(lambda: self._tolist_using_iter(source_data), number=5)
        print(iter_time)
        todict_time = timeit.timeit(lambda: self._tolist_using_to_dict(source_data), number=5)
        print(todict_time)

        # assert that iter_df_as_dicts is at least 1.5x faster (in pandas 2.0.0 it is ~2x faster but allow margin for error)
        self.assertLess(iter_time, todict_time / 1.5)

        assert 0
