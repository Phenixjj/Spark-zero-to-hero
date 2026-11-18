"""Module 5 — Qualité des données et sortie reproductible.

Retourner toutes les commandes avec seulement order_id et error (string).
error vaut, par priorité : id_manquant si order_id est NULL ; client_invalide
si customer_id est NULL ou <= 0 ; date_manquante si order_date est NULL ;
statut_invalide si status n'est pas completed ou cancelled (NULL compris) ;
montant_invalide si amount est NULL ou négatif ; sinon NULL.
Le lanceur ajoute des données invalides, vérifie les erreurs puis écrit les
lignes valides deux fois en Parquet overwrite et vérifie le résultat relu.
"""

from datetime import date
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from common import DATA, D, ORDERS_SCHEMA, load_data, start_spark, verify


def solve(customers, orders):
    raise NotImplementedError("À vous de jouer !")


def run(solution=solve):
    spark = start_spark()
    output = DATA / "output_production"
    created = False
    try:
        customers, orders = load_data(spark)
        extra = spark.createDataFrame([
            (None, 1, date(2024, 1, 1), D("1.00"), "completed"),
            (109, 0, date(2024, 1, 1), D("1.00"), "completed"),
            (110, 1, None, D("1.00"), "completed"),
            (111, 1, date(2024, 1, 1), D("1.00"), None),
            (112, 1, date(2024, 1, 1), D("-1.00"), "completed"),
        ], ORDERS_SCHEMA)
        result = solution(customers, orders.unionByName(extra))
        columns = [("order_id", IntegerType()), ("error", StringType())]
        # Pas de tri Python sur des valeurs NULL et entières mélangées.
        valid = [(i, None) for i in (101, 102, 103, 104, 105, 106, 108)]
        invalid = [(107, "montant_invalide"), (109, "client_invalide"),
                   (110, "date_manquante"), (111, "statut_invalide"),
                   (112, "montant_invalide")]
        verify(result.filter(F.col("order_id").isNotNull()), columns, valid + invalid)
        assert [tuple(r) for r in result.filter(F.col("order_id").isNull()).collect()] == [
            (None, "id_manquant")
        ]
        output.mkdir(exist_ok=False)
        created = True
        clean = result.filter(F.col("error").isNull())
        for _ in range(2):
            clean.write.mode("overwrite").parquet(str(output))
        verify(spark.read.parquet(str(output)), columns, valid)
        print("Module 5 : assertions qualité et Parquet validées")
    finally:
        spark.stop()
        if created:
            shutil.rmtree(output)


if __name__ == "__main__":
    run()
