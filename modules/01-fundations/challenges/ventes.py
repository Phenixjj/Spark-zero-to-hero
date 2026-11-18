"""Défi : calculer les ventes valides par client."""

from pyspark.sql import functions as F


def ventes_par_client(commandes, clients):
    """Conserver les montants strictement positifs et les clients connus.

    Retourner client_id, nom, nb_commandes et total. Un client sans commande
    valide reste présent avec zéro ; les identifiants clients sont uniques.
    """
    ventes = (
        commandes.filter(F.col("montant") > 0)
        .groupBy("client_id")
        .agg(F.count("*").alias("nb_commandes"), F.sum("montant").alias("total"))
    )
    return (
        clients.select("client_id", "nom")
        .join(ventes, "client_id", "left")
        .fillna({"nb_commandes": 0, "total": 0.0})
        .select("client_id", "nom", "nb_commandes", "total")
    )
