import pytest
from pyspark.sql import SparkSession, types as T

from ventes import ventes_par_client


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]")
               .appName("test-fondations")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_ventes_et_clients_sans_ventes(spark):
    clients = spark.createDataFrame([(1, "Ada"), (2, "Lin"), (3, "Sam")],
                                    ["client_id", "nom"])
    commandes = spark.createDataFrame(
        [(1, 10.0), (1, 2.5), (1, None), (2, -3.0), (2, 0.0), (99, 100.0)],
        "client_id long, montant double",
    )
    resultat = {r.client_id: (r.nom, r.nb_commandes, r.total)
                for r in ventes_par_client(commandes, clients).collect()}
    assert resultat == {1: ("Ada", 2, 12.5), 2: ("Lin", 0, 0.0), 3: ("Sam", 0, 0.0)}


def test_entrees_vides_et_schema(spark):
    clients = spark.createDataFrame([], "client_id long, nom string")
    commandes = spark.createDataFrame([], "client_id long, montant double")
    resultat = ventes_par_client(commandes, clients)
    assert resultat.columns == ["client_id", "nom", "nb_commandes", "total"]
    assert isinstance(resultat.schema["nb_commandes"].dataType, T.LongType)
    assert resultat.collect() == []
