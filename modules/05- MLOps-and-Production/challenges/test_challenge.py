"""Test de départ à compléter pendant le défi."""

from chispa import assert_df_equality

from pipeline import VENTES_SCHEMA, ventes_valides


def test_filtrage_conserve_les_doublons_valides(spark):
    ventes = spark.createDataFrame(
        [
            (" Café ", 1.0, 0.0),
            (" Café ", 1.0, 0.0),
            ("", 1.0, 1.0),
            ("thé", -1.0, 2.0),
        ],
        VENTES_SCHEMA,
    )
    attendu = spark.createDataFrame(
        [("café", 1.0, 0.0), ("café", 1.0, 0.0)], VENTES_SCHEMA
    )
    assert_df_equality(ventes_valides(ventes), attendu, ignore_row_order=True)
