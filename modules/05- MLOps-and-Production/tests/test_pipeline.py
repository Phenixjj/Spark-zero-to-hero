from chispa import assert_approx_df_equality, assert_df_equality

from pipeline import VENTES_SCHEMA, chiffre_affaires_par_produit, ventes_valides


def test_normalisation_et_filtrage(spark):
    ventes = spark.createDataFrame(
        [
            (" Café ", 2.0, 3.5),
            ("THÉ", 1.0, 0.0),
            (" ", 1.0, 1.0),
            (None, 1.0, 1.0),
            ("café", None, 1.0),
            ("café", 0.0, 1.0),
            ("café", -1.0, 1.0),
            ("café", 1.0, None),
            ("café", 1.0, -1.0),
            ("café", float("nan"), 1.0),
            ("café", 1.0, float("nan")),
            ("café", float("inf"), 1.0),
            ("café", 1.0, float("inf")),
            ("café", float("-inf"), 1.0),
            ("café", 1.0, float("-inf")),
        ],
        VENTES_SCHEMA,
    )
    attendu = spark.createDataFrame(
        [("café", 2.0, 3.5), ("thé", 1.0, 0.0)], VENTES_SCHEMA
    )
    assert_df_equality(ventes_valides(ventes), attendu, ignore_row_order=True)


def test_agregation_et_arrondi(spark):
    ventes = spark.createDataFrame(
        [
            (" Café ", 2.0, 3.5),
            ("café", 1.0, 3.5),
            ("thé", 3.0, 0.333),
            ("thé", -1.0, 100.0),
        ],
        VENTES_SCHEMA,
    )
    attendu = spark.createDataFrame(
        [("café", 10.5), ("thé", 1.0)], "produit string, ca double"
    )
    assert_approx_df_equality(
        chiffre_affaires_par_produit(ventes),
        attendu,
        precision=0.000001,
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_entree_vide_et_aucune_vente_valide(spark):
    vide = spark.createDataFrame([], VENTES_SCHEMA)
    invalide = spark.createDataFrame([(" ", 0.0, 2.0)], VENTES_SCHEMA)
    attendu = spark.createDataFrame([], "produit string, ca double")
    for ventes in (vide, invalide):
        assert_df_equality(
            chiffre_affaires_par_produit(ventes),
            attendu,
            ignore_nullable=True,
            ignore_row_order=True,
        )
