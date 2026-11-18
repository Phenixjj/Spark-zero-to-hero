from scripts.kafka_stream import decode_events


def test_decode_kafka_records_without_broker(spark):
    records = spark.createDataFrame(
        [("events", 0, 7, b'{"event_id":"a","category":"x","event_time":"2024-01-01 00:01:00","value":2}'),
         ("events", 0, 8, b"not-json"),
         ("events", 1, 9, b'{"event_id":"b","category":"x","event_time":"invalid","value":1}'),
         ("events", 1, 10, b'{"event_id":"c","category":"x","event_time":"2024-01-01 00:01:00","value":-1}')],
        "topic string, partition int, offset long, value binary",
    )
    rows = decode_events(records).collect()
    assert [(r.event_id, r.topic, r.partition, r.offset, r.value) for r in rows] == [
        ("a", "events", 0, 7, 2.0)
    ]
