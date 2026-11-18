"""Module 2 — Performance : plan et réduction des données avant shuffle.

Filtrer les commandes completed avec montant non NULL, ne garder que customer_id
et amount, puis repartitionner en deux partitions par customer_id. Calculer par
client nb_commandes (long) et total (decimal(20,2)). Examiner le plan formaté :
repérer les filtres et projections, l'échange et l'agrégation. Ce minuscule jeu
ne permet pas de mesurer une accélération : repartition ajoute aussi un shuffle.
"""

from pyspark.sql.types import DecimalType, IntegerType, LongType
from common import D, load_data, start_spark, verify


def solve(customers, orders):
    raise NotImplementedError("À vous de jouer !")


def run(solution=solve):
    spark = start_spark()
    try:
        customers, orders = load_data(spark)
        result = solution(customers, orders)
        result.explain("formatted")
        verify(result, [
            ("customer_id", IntegerType()), ("nb_commandes", LongType()),
            ("total", DecimalType(20, 2)),
        ], [
            (1, 2, D("125.00")), (2, 2, D("100.00")), (3, 2, D("100.00")),
        ])
        print("Module 2 : assertions validées")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
