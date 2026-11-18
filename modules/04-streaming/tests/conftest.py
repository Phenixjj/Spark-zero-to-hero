import shutil
import sys
import uuid
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

MODULE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE))


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("streaming-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def workdir():
    path = MODULE / (".test-streaming-" + uuid.uuid4().hex)
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)
