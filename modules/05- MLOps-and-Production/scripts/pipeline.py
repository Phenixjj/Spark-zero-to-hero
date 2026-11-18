"""Transformations Spark pures pour le module MLOps."""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


VENTES_SCHEMA = StructType(
    [
        StructField("produit", StringType(), True),
        StructField("quantite", DoubleType(), True),
        StructField("prix_unitaire", DoubleType(), True),
    ]
)


def ventes_valides(ventes: DataFrame) -> DataFrame:
    """Normalise les produits et écarte les ventes incomplètes ou invalides."""
    normalisees = ventes.select(
        F.lower(F.trim(F.col("produit"))).alias("produit"),
        F.col("quantite"),
        F.col("prix_unitaire"),
    )
    return normalisees.filter(
        F.col("produit").isNotNull()
        & (F.length("produit") > 0)
        & F.col("quantite").isNotNull()
        & ~F.isnan("quantite")
        & (F.col("quantite") > 0)
        & (F.col("quantite") != float("inf"))
        & F.col("prix_unitaire").isNotNull()
        & ~F.isnan("prix_unitaire")
        & (F.col("prix_unitaire") >= 0)
        & (F.col("prix_unitaire") != float("inf"))
    )


def chiffre_affaires_par_produit(ventes: DataFrame) -> DataFrame:
    """Calcule le chiffre d'affaires arrondi à deux décimales par produit."""
    return (
        ventes_valides(ventes)
        .groupBy("produit")
        .agg(F.round(F.sum(F.col("quantite") * F.col("prix_unitaire")), 2).alias("ca"))
    )
