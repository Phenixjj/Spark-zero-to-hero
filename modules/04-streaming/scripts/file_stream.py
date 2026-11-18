"""Reprendre un flux de fichiers à partir de son checkpoint."""

import json
from pathlib import Path
import shutil

from pyspark.sql import SparkSession


def write_batch(path, rows):
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")


def main():
    root = Path(".spark-streaming-example")
    root.mkdir(exist_ok=False)
    spark = None
    try:
        source = root / "input"
        source.mkdir()
        write_batch(source / "one.json", [{"id": 1}, {"id": 2}])
        spark = (SparkSession.builder.master("local[2]").appName("file-stream")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        stream = spark.readStream.schema("id int").json(str(source))

        def run():
            query = (stream.writeStream.format("parquet").outputMode("append")
                     .option("path", str(root / "output"))
                     .option("checkpointLocation", str(root / "checkpoint"))
                     .trigger(availableNow=True).start())
            query.awaitTermination()

        run()
        write_batch(source / "two.json", [{"id": 3}])
        run()
        assert sorted(row.id for row in spark.read.parquet(str(root / "output")).collect()) == [1, 2, 3]
        print("Reprise et sortie sans rejeu vérifiées.")
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(root)


if __name__ == "__main__":
    main()
