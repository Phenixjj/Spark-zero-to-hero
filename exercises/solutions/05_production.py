"""Correction du module 5."""

import importlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F


def solve(customers, orders):
    # L'ordre des when définit une seule raison reproductible par ligne.
    error = (F.when(F.col("order_id").isNull(), "id_manquant")
             .when(F.col("customer_id").isNull() | (F.col("customer_id") <= 0),
                   "client_invalide")
             .when(F.col("order_date").isNull(), "date_manquante")
             .when(F.col("status").isNull() | ~F.col("status").isin("completed", "cancelled"),
                   "statut_invalide")
             .when(F.col("amount").isNull() | (F.col("amount") < 0), "montant_invalide")
             .otherwise(F.lit(None).cast("string")))
    return orders.select("order_id", error.alias("error"))


if __name__ == "__main__":
    importlib.import_module(Path(__file__).stem).run(solve)
