from pyspark.sql.types import DoubleType

from scripts.udf_and_arrow import comparisons


def test_native_python_and_pandas_udfs_are_equivalent(spark):
    expected = {1: 5.0, 2: None, 3: -6.0, 4: 0.0, 5: 3.0}
    for result in comparisons(spark).values():
        assert isinstance(result.schema["result"].dataType, DoubleType)
        assert {row.id: row.result for row in result.collect()} == expected
