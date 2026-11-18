"""Exemple autonome : transformations, jointure et agrégation."""

from pyspark.sql import SparkSession, functions as F, types as T


def build_report(spark):
    schema = T.StructType([
        T.StructField("commande_id", T.IntegerType(), False),
        T.StructField("client_id", T.IntegerType(), False),
        T.StructField("montant", T.DoubleType(), True),
    ])
    commandes = spark.createDataFrame(
        [(1, 10, 20.0), (2, 10, None), (3, 20, 15.0), (4, 30, -1.0)],
        schema,
    )
    clients = spark.createDataFrame(
        [(10, "Paris"), (20, "Lyon")], ["client_id", "ville"]
    )
    return (
        commandes.filter(F.col("montant") > 0)
        .join(clients, "client_id", "inner")
        .groupBy("ville")
        .agg(F.count("*").alias("commandes"), F.sum("montant").alias("total"))
    )


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("fondations")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rapport = build_report(spark)
        rapport.explain()
        resultat = {r.ville: (r.commandes, r.total) for r in rapport.collect()}
        assert resultat == {"Paris": (1, 20.0), "Lyon": (1, 15.0)}
        print("Rapport vérifié :", resultat)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
