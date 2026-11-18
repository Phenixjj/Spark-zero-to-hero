"""Observer une jointure broadcast et une fenêtre de classement."""

from pyspark.sql import SparkSession, Window, functions as F


def main():
    spark = (SparkSession.builder.master("local[2]").appName("advanced-plans")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.adaptive.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        orders = spark.createDataFrame(
            [(1, "a", 20), (2, "a", 20), (3, "b", 5)],
            "order_id int, customer string, amount int",
        )
        customers = spark.createDataFrame(
            [("a", "Nord"), ("b", "Sud")], "customer string, region string"
        )
        joined = orders.join(F.broadcast(customers), "customer")
        window = Window.partitionBy("region").orderBy(
            F.desc("amount"), F.asc("order_id")
        )
        ranked = joined.withColumn("position", F.row_number().over(window))
        ranked.explain("formatted")
        rows = {(r.order_id, r.region, r.position) for r in ranked.collect()}
        assert rows == {(1, "Nord", 1), (2, "Nord", 2), (3, "Sud", 1)}
        print("Jointure et classement vérifiés.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
