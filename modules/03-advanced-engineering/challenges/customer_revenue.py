from pyspark.sql import Window, functions as F


def clean_events(events):
    """Conserver un événement valide par identifiant, le plus récent d'abord."""
    valid = events.filter(
        F.col("event_id").isNotNull()
        & (F.length(F.trim(F.col("event_id"))) > 0)
        & F.col("customer_id").isNotNull()
        & (F.length(F.trim(F.col("customer_id"))) > 0)
        & F.col("event_time").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0)
    )
    order = Window.partitionBy("event_id").orderBy(
        F.desc("event_time"), F.desc("amount"), F.asc("customer_id")
    )
    return (valid.withColumn("_row", F.row_number().over(order))
            .filter(F.col("_row") == 1).drop("_row"))


def customer_revenue(customers, events):
    """Revenu et rang de chaque client, y compris ceux sans événement valide."""
    totals = clean_events(events).groupBy("customer_id").agg(
        F.sum("amount").alias("revenue")
    )
    ranked = customers.join(totals, "customer_id", "left").fillna({"revenue": 0})
    order = Window.partitionBy("region").orderBy(
        F.desc("revenue"), F.asc("customer_id")
    )
    return ranked.withColumn("position", F.row_number().over(order))
