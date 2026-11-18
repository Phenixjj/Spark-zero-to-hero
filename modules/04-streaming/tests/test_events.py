import json
import uuid
from datetime import datetime

from challenges.events import prepare_events, unique_events, window_counts

SCHEMA = "event_id string, category string, event_time string, value int"


def write_rows(path, rows):
    with path.open("w", encoding="utf-8") as file:
        for event_id, category, event_time, value in rows:
            file.write(json.dumps(dict(event_id=event_id, category=category,
                                       event_time=event_time, value=value)) + "\n")


def test_prepare_events(spark):
    events = spark.createDataFrame(
        [(" a ", " x ", "2024-01-01 00:01:00", 0),
         ("", "x", "2024-01-01 00:01:00", 1),
         ("b", None, "2024-01-01 00:01:00", 1),
         ("c", "x", "invalid", 1), ("d", "x", None, 1),
         ("e", "x", "2024-01-01 00:01:00", None),
         ("f", "x", "2024-01-01 00:01:00", -1)], SCHEMA,
    )
    rows = prepare_events(events).collect()
    assert [(r.event_id, r.category, r.value) for r in rows] == [("a", "x", 0)]


def test_window_counts_on_stream(spark, workdir):
    source = workdir / "input"
    source.mkdir()
    write_rows(source / "one.json", [
        ("a", "x", "2024-01-01 00:01:00", 1),
        ("a", "x", "2024-01-01 00:01:00", 1),
        ("b", "x", "2024-01-01 00:04:59", 1),
        ("c", "x", "2024-01-01 00:05:00", 1),
    ])
    stream = spark.readStream.schema(SCHEMA).json(str(source))
    name = "counts_" + uuid.uuid4().hex
    query = (window_counts(stream).writeStream.format("memory")
             .queryName(name).outputMode("complete")
             .option("checkpointLocation", str(workdir / "checkpoint"))
             .start())
    try:
        query.processAllAvailable()
        write_rows(source / "two.json", [
            ("a", "x", "2024-01-01 00:01:00", 1),
            ("d", "x", "2024-01-01 00:03:00", 1),
        ])
        query.processAllAvailable()
        rows = {(r.window.start, r.category, r["count"])
                for r in spark.table(name).collect()}
        assert rows == {(datetime(2024, 1, 1, 0, 0), "x", 3),
                        (datetime(2024, 1, 1, 0, 5), "x", 1)}
    finally:
        query.stop()
        spark.catalog.dropTempView(name)


def test_deduplication_survives_restart(spark, workdir):
    source = workdir / "input"
    source.mkdir()
    write_rows(source / "one.json", [("a", "x", "2024-01-01 00:01:00", 1)])
    stream = unique_events(spark.readStream.schema(SCHEMA).json(str(source)))

    def run():
        query = (stream.writeStream.format("parquet").outputMode("append")
                 .option("path", str(workdir / "output"))
                 .option("checkpointLocation", str(workdir / "checkpoint"))
                 .trigger(availableNow=True).start())
        query.awaitTermination()

    run()
    write_rows(source / "two.json", [
        ("a", "x", "2024-01-01 00:01:00", 1),
        ("b", "x", "2024-01-01 00:02:00", 2),
    ])
    run()
    assert sorted(r.event_id for r in spark.read.parquet(str(workdir / "output")).collect()) == ["a", "b"]
