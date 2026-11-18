"""Chargement typé et vérifications partagées des exercices."""

from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DATA = Path(__file__).resolve().parent / "data"
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
])
ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
])


def start_spark():
    spark = (SparkSession.builder.master("local[2]")
             .appName("Spark zero to hero - exercices")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_data(spark):
    if not (DATA / "customers.csv").exists() or not (DATA / "orders/orders.csv").exists():
        raise FileNotFoundError("Exécutez d'abord python exercises/generate_data.py")
    reader = spark.read.option("header", True).option("mode", "FAILFAST")
    customers = reader.schema(CUSTOMERS_SCHEMA).csv(str(DATA / "customers.csv"))
    orders = reader.schema(ORDERS_SCHEMA).csv(str(DATA / "orders"))
    assert customers.schema == CUSTOMERS_SCHEMA
    assert orders.schema == ORDERS_SCHEMA
    return customers, orders


def verify(result, columns, expected):
    """Compare colonnes, types et valeurs après un tri explicite côté Python."""
    assert result.columns == [name for name, _ in columns], result.columns
    assert [field.dataType for field in result.schema] == [
        dtype for _, dtype in columns
    ], result.schema
    actual = [tuple(row) for row in result.collect()]
    assert sorted(actual) == sorted(expected), (actual, expected)


D = Decimal
