from pyspark.sql import functions as F


def prepare_events(events):
    """Valider les champs et convertir l'heure en timestamp."""
    normalized = (events.withColumn("event_id", F.trim("event_id"))
                  .withColumn("category", F.trim("category"))
                  .withColumn("event_time", F.to_timestamp("event_time")))
    return normalized.filter(
        F.col("event_id").isNotNull() & (F.length("event_id") > 0)
        & F.col("category").isNotNull() & (F.length("category") > 0)
        & F.col("event_time").isNotNull()
        & F.col("value").isNotNull() & (F.col("value") >= 0)
    )


def unique_events(events):
    """Limiter l'état de déduplication à l'horizon du watermark."""
    return (prepare_events(events).withWatermark("event_time", "10 minutes")
            .dropDuplicatesWithinWatermark(["event_id"]))


def window_counts(events):
    """Compter par fenêtre et catégorie après déduplication."""
    return unique_events(events).groupBy(
        F.window("event_time", "5 minutes"), "category"
    ).count()
