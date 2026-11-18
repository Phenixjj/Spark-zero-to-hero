"""Exécution locale reproductible : python scripts/run_pipeline.py."""

from pyspark.sql import SparkSession

from pipeline import VENTES_SCHEMA, chiffre_affaires_par_produit, ventes_valides


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-mlops-demo")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ventes = spark.createDataFrame(
            [
                (" Café ", 2.0, 3.5),
                ("café", 1.0, 3.5),
                ("Thé", 2.0, 2.0),
                ("", 1.0, 10.0),
                ("Thé", -1.0, 2.0),
            ],
            VENTES_SCHEMA,
        )
        resultat = chiffre_affaires_par_produit(ventes)
        assert ventes_valides(ventes).count() == 3
        assert {ligne["produit"]: ligne["ca"] for ligne in resultat.collect()} == {
            "café": 10.5,
            "thé": 4.0,
        }
        resultat.orderBy("produit").show()
        print("Assertions réussies.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
