"""Module 3 — Ingénierie avancée : jointures, agrégations et fenêtres.

Par client, calculer le nombre de commandes completed et la somme de leurs
montants non NULL. Conserver les clients sans commande ; une somme entièrement
NULL vaut 0. Retourner customer_id, name, nb_commandes (long), total
(decimal(20,2)), rang (int). Classer par total décroissant, puis customer_id
croissant en cas d'égalité, avec row_number.
"""

from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType
from common import D, load_data, start_spark, verify


def solve(customers, orders):
    raise NotImplementedError("À vous de jouer !")


def run(solution=solve):
    spark = start_spark()
    try:
        customers, orders = load_data(spark)
        verify(solution(customers, orders), [
            ("customer_id", IntegerType()), ("name", StringType()),
            ("nb_commandes", LongType()), ("total", DecimalType(20, 2)),
            ("rang", IntegerType()),
        ], [
            (1, "Alice", 2, D("125.00"), 1), (2, "Bob", 2, D("100.00"), 2),
            (3, "Chloé", 2, D("100.00"), 3), (4, "David", 1, D("0.00"), 4),
            (5, "Eva", 0, D("0.00"), 5),
        ])
        print("Module 3 : assertions validées")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
