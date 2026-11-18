"""Exemple Kafka optionnel : décoder les événements JSON vers la console."""

import argparse

from pyspark.sql import SparkSession, functions as F

EVENT_SCHEMA = "event_id STRING, category STRING, event_time STRING, value DOUBLE"


def decode_events(records):
    decoded = records.select(
        "topic", "partition", "offset",
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    ).select(
        "topic", "partition", "offset", "event.*",
    ).withColumn("event_time", F.to_timestamp("event_time"))
    return decoded.filter(
        F.col("event_id").isNotNull() & (F.length(F.trim("event_id")) > 0)
        & F.col("category").isNotNull() & (F.length(F.trim("category")) > 0)
        & F.col("event_time").isNotNull() & F.col("value").isNotNull()
        & (F.col("value") >= 0)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--checkpoint", required=True)
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("kafka-json-demo")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    query = None
    try:
        records = (spark.readStream.format("kafka")
                   .option("kafka.bootstrap.servers", args.bootstrap_servers)
                   .option("subscribe", args.topic)
                   .option("startingOffsets", "earliest")
                   .load())
        query = (decode_events(records).writeStream.format("console")
                 .outputMode("append")
                 .option("truncate", "false")
                 .option("checkpointLocation", args.checkpoint)
                 .start())
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
