"""Correction du module 4."""

import importlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F


def solve(customers, orders):
    # Les transformations sont paresseuses : le lanceur démarre le flux.
    return (orders.filter((F.col("status") == "completed") & F.col("amount").isNotNull())
            .select("order_id", "amount"))


if __name__ == "__main__":
    importlib.import_module(Path(__file__).stem).run(solve)
