"""The type fixture must reject changes between SQL NULL and JSON null values."""
import itertools
import unittest
from unittest.mock import Mock

from type_compat import TypeRun


class JsonNullComparisonTests(unittest.TestCase):
    def test_distinct_null_representations_cannot_compare_equal(self):
        for expected, actual in itertools.permutations((None, "null", '"null"'), 2):
            with self.subTest(expected=expected, actual=actual):
                run = TypeRun.__new__(TypeRun)
                run.columns = [("doc", "String")]
                run.pg = Mock()
                run.pg.execute.return_value.fetchall.return_value = [(expected,)]
                run.table = Mock(return_value={})
                run.rows = Mock(return_value=[(actual,)])
                with self.assertRaisesRegex(AssertionError, "complete source/target rows differ"):
                    run.compare("null-regression")


if __name__ == "__main__":
    unittest.main()
