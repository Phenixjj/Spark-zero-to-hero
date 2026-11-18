"""Comparer une expression native, une UDF Python et une Pandas UDF."""

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType


@F.udf(returnType=DoubleType(), useArrow=False)
def double_python(value):
    return None if value is None else value * 2


@F.pandas_udf(DoubleType())
def double_pandas(values: pd.Series) -> pd.Series:
    return values * 2


def comparisons(spark):
    data = spark.createDataFrame(
        [(1, 2.5), (2, None), (3, -3.0), (4, 0.0), (5, 1.5)],
        "id int, value double",
    ).coalesce(1)
    return {
        "native": data.select("id", (F.col("value") * 2).alias("result")),
        "python": data.select("id", double_python("value").alias("result")),
        "pandas": data.select("id", double_pandas("value").alias("result")),
    }


def main():
    spark = (SparkSession.builder.master("local[2]").appName("udf-arrow")
             .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        expected = {1: 5.0, 2: None, 3: -6.0, 4: 0.0, 5: 3.0}
        for name, result in comparisons(spark).items():
            print(f"\nPlan {name} :")
            result.explain("formatted")
            assert {row.id: row.result for row in result.collect()} == expected
        print("Les trois implémentations produisent les mêmes résultats.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
