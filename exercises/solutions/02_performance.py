"""Correction du module 2."""

import importlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F


def solve(customers, orders):
    # Réduire lignes et colonnes avant l'échange limite les données à déplacer.
    reduced = (orders.filter((F.col("status") == "completed") & F.col("amount").isNotNull())
               .select("customer_id", "amount")
               .repartition(2, "customer_id"))
    return reduced.groupBy("customer_id").agg(
        F.count("*").alias("nb_commandes"), F.sum("amount").alias("total"))


if __name__ == "__main__":
    importlib.import_module(Path(__file__).stem).run(solve)
