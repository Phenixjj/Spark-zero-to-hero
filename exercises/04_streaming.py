"""Module 4 — Structured Streaming borné sur fichiers.

orders est ici un DataFrame streaming avec le même schéma que le batch.
Retourner order_id et amount pour les commandes completed avec montant non NULL.
Le lanceur utilise un déclenchement availableNow et un checkpoint local :
il traite les fichiers existants puis s'arrête. Les fichiers d'entrée doivent
être immuables une fois déposés. Pas de collect() sur le DataFrame streaming.
"""

import shutil
from pyspark.sql.types import DecimalType, IntegerType
from common import DATA, D, ORDERS_SCHEMA, start_spark, verify


def solve(customers, orders):
    raise NotImplementedError("À vous de jouer !")


def run(solution=solve):
    spark = start_spark()
    checkpoint = DATA / "checkpoint_streaming"
    query = None
    created = False
    try:
        if not (DATA / "orders/orders.csv").exists():
            raise FileNotFoundError("Exécutez d'abord python exercises/generate_data.py")
        # Refuser d'effacer un checkpoint préexistant appartenant à un autre run.
        checkpoint.mkdir(exist_ok=False)
        created = True
        orders = (spark.readStream.schema(ORDERS_SCHEMA)
                  .option("header", True).option("mode", "FAILFAST")
                  .csv(str(DATA / "orders")))
        result = solution(None, orders)
        assert result.isStreaming
        batches = []

        def check_batch(batch, batch_id):
            verify(batch, [
                ("order_id", IntegerType()), ("amount", DecimalType(10, 2)),
            ], [
                (101, D("100.00")), (103, D("80.00")), (104, D("20.00")),
                (105, D("40.00")), (106, D("60.00")), (108, D("25.00")),
            ])
            batches.append(batch_id)

        query = (result.writeStream.foreachBatch(check_batch)
                 .option("checkpointLocation", str(checkpoint))
                 .trigger(availableNow=True).start())
        assert query.awaitTermination(90), "Le flux ne s'est pas terminé en 90 secondes"
        assert query.exception() is None, query.exception()
        assert batches == [0], batches
        print("Module 4 : assertions streaming validées")
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()
        if created:
            shutil.rmtree(checkpoint)


if __name__ == "__main__":
    run()
