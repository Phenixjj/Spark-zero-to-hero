"""Défi : agréger des événements avant de joindre une petite dimension."""

from pyspark.sql import functions as F


def compter_par_categorie(evenements, categories):
    """Compter les événements par catégorie connue présente dans les événements.

    Les identifiants de categories sont uniques ; les identifiants d'événements
    inconnus ou nuls ne sont pas inclus. Le résultat ne promet aucun ordre.
    """
    comptes = evenements.groupBy("categorie_id").count()
    return (
        comptes.join(F.broadcast(categories), "categorie_id", "inner")
        .select("categorie_id", "nom", F.col("count").alias("nb_evenements"))
    )
