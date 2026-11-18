# Défi : qualité et traçabilité des ventes

Dans une copie ou une extension du module, ajouter une transformation produisant les ventes rejetées avec une colonne `raison`. Définir une priorité déterministe si plusieurs champs sont invalides : `produit_invalide`, puis `quantite_invalide`, puis `prix_invalide`. Une vente rejetée apparaît une seule fois ; toutes les autres restent valides. Conserver la ligne d'origine pour diagnostiquer la cause, sans exposer de données sensibles dans les logs.

Le test de départ `test_challenge.py` est exécutable dès maintenant ; il vérifie le contrat existant sur un jeu contenant des doublons et des lignes rejetées. Il ne valide pas encore la nouvelle colonne `raison` : compléter les tests en développant la solution.

Ajouter des tests pytest/chispa couvrant chaque raison, plusieurs erreurs sur une même ligne, zéro en prix, `null`, NaN, infini et entrée vide. Vérifier que le nombre de lignes valides plus rejetées égale le nombre d'entrées, puis tester le chiffre d'affaires avec uniquement les lignes valides. Décrire dans une note comment publier le taux de rejet et quel seuil déclencherait une alerte.

Depuis le dossier du module, exécuter :

```bash
python -m pytest -q
python scripts/run_pipeline.py
```
