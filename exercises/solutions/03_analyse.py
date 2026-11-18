"""Correction du module 3."""

import importlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import Window, functions as F


def solve(customers, orders):
    # Filtrer avant la jointure externe pour garder aussi les clients sans commande.
    completed = orders.filter(F.col("status") == "completed")
    grouped = (customers.join(completed, "customer_id", "left")
               .groupBy("customer_id", "name")
               .agg(F.count("order_id").alias("nb_commandes"),
                    F.sum("amount").alias("total")))
    zero = F.lit(0).cast("decimal(20,2)")
    with_total = grouped.withColumn("total", F.coalesce("total", zero))
    # Le second critère rend l'ordre déterministe à total égal.
    window = Window.orderBy(F.desc("total"), F.asc("customer_id"))
    return with_total.select("customer_id", "name", "nb_commandes", "total",
                             F.row_number().over(window).alias("rang"))


if __name__ == "__main__":
    importlib.import_module(Path(__file__).stem).run(solve)
