import pytest
from pyspark.sql import SparkSession

from categories import compter_par_categorie


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]")
               .appName("test-performance")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_comptage_et_categories_sans_evenement(spark):
    evenements = spark.createDataFrame([(1,), (1,), (2,), (99,), (None,)],
                                       "categorie_id long")
    categories = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")],
                                       "categorie_id long, nom string")
    resultat = compter_par_categorie(evenements, categories)
    assert resultat.columns == ["categorie_id", "nom", "nb_evenements"]
    assert {r.categorie_id: (r.nom, r.nb_evenements) for r in resultat.collect()} == {
        1: ("A", 2), 2: ("B", 1)
    }


def test_entrees_vides(spark):
    evenements = spark.createDataFrame([], "categorie_id long")
    categories = spark.createDataFrame([(1, "A")], "categorie_id long, nom string")
    assert compter_par_categorie(evenements, categories).collect() == []
    categories_vides = spark.createDataFrame([], "categorie_id long, nom string")
    assert compter_par_categorie(evenements, categories_vides).collect() == []


def test_broadcast_utilise_pour_la_dimension(spark):
    evenements = spark.createDataFrame([(1,)], "categorie_id long")
    categories = spark.createDataFrame([(1, "A")], "categorie_id long, nom string")
    plan = compter_par_categorie(evenements, categories)._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan
    assert "BuildRight" in plan
