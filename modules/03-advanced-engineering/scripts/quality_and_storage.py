"""Valider puis relire des données Parquet partitionnées."""

from pathlib import Path
import shutil

from pyspark.sql import SparkSession, functions as F


def main():
    root = Path(".spark-advanced-example")
    root.mkdir(exist_ok=False)
    spark = None
    try:
        spark = (SparkSession.builder.master("local[2]").appName("advanced-storage")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        data = spark.createDataFrame(
            [(1, "Nord", 10), (2, "Sud", -1), (3, "Nord", None)],
            "id int, region string, amount int",
        )
        valid = data.filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        rejected = data.filter(F.col("amount").isNull() | (F.col("amount") < 0))
        assert rejected.count() == 2
        path = str(root / "orders")
        valid.write.mode("error").partitionBy("region").parquet(path)
        read_back = spark.read.parquet(path).filter(F.col("region") == "Nord")
        assert {(r.id, r.amount) for r in read_back.collect()} == {(1, 10)}
        read_back.explain("formatted")
        print("Qualité et lecture Parquet vérifiées.")
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(root)


if __name__ == "__main__":
    main()
