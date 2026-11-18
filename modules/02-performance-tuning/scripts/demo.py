"""Observer une jointure broadcast, un shuffle et un cache matérialisé."""

from pyspark.sql import SparkSession, functions as F


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("performance")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    evenements = None
    try:
        evenements = (
            spark.range(100)
            .select(F.col("id"), (F.col("id") % 4).alias("categorie_id"))
            .repartition(2, "categorie_id")
            .cache()
        )
        assert evenements.count() == 100  # Matérialise toutes les partitions du cache.
        categories = spark.createDataFrame(
            [(0, "A"), (1, "B"), (2, "C"), (3, "D")],
            ["categorie_id", "nom"],
        )
        rapport = (
            evenements.join(F.broadcast(categories), "categorie_id")
            .groupBy("nom").count()
        )
        rapport.explain(mode="formatted")
        resultat = {r.nom: r["count"] for r in rapport.collect()}
        assert resultat == {"A": 25, "B": 25, "C": 25, "D": 25}
        print("Comptages vérifiés :", resultat)
    finally:
        if evenements is not None:
            evenements.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
