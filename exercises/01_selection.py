"""Module 1 — Lecture et sélection.

À partir de orders, conserver les commandes completed dont le montant n'est pas
NULL, puis retourner seulement order_id et amount, dans cet ordre. Ne pas modifier
les types. Utiliser les opérations DataFrame ; aucune collecte n'est nécessaire.
"""

from pyspark.sql.types import DecimalType, IntegerType

from common import D, load_data, start_spark, verify


def solve(customers, orders):
    raise NotImplementedError("À vous de jouer !")


def run(solution=solve):
    spark = start_spark()
    try:
        customers, orders = load_data(spark)
        verify(solution(customers, orders), [
            ("order_id", IntegerType()), ("amount", DecimalType(10, 2)),
        ], [
            (101, D("100.00")), (103, D("80.00")), (104, D("20.00")),
            (105, D("40.00")), (106, D("60.00")), (108, D("25.00")),
        ])
        print("Module 1 : assertions validées")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
