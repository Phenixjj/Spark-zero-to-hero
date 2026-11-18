# Exercices PySpark 3.5

Parcours local en cinq modules, utilisant l'API DataFrame. Les résultats
attendus et leurs types sont vérifiés par des assertions ; les corrections sont
commentées.

| Module | Exercice | Objectifs |
|---|---|---|
| 1 — Fondamentaux | `01_selection.py` | Schéma, lecture, filtres et sélection |
| 2 — Performance tuning | `02_performance.py` | Projection, filtrage, partitionnement et plan d'exécution |
| 3 — Advanced engineering | `03_analyse.py` | Jointure externe, agrégation, NULL et fenêtre |
| 4 — Streaming | `04_streaming.py` | Source fichiers, Structured Streaming et traitement borné |
| 5 — MLOps and Production | `05_production.py` | Validation, rejets et écriture Parquet reproductible |

## Préparation

Depuis la racine du dépôt, avec Python 3.9 à 3.12 et Java 8, 11 ou 17 installés :

```bash
python3 -m venv exercises/.venv
exercises/.venv/bin/python -m pip install -r requirements.txt
exercises/.venv/bin/python exercises/generate_data.py
```

Le générateur utilise uniquement la bibliothèque standard. Il réécrit de façon
identique les deux CSV UTF-8 ignorés par Git dans `exercises/data/` ; il n'utilise
ni aléa ni date courante. L'exécution ne nécessite ni cluster ni téléchargement de données.
Les schémas explicites sont dans `common.py` : dates, montants décimaux et ID
numériques ne dépendent pas de l'inférence. Le montant vide de la commande 107
est un `NULL` ; Eva n'a aucune commande. Le statut `cancelled` permet de vérifier
les filtres. Les montants sont en euros, sans conversion monétaire.

## Utilisation

Pour chaque module, lisez les consignes dans son fichier `NN_*.py`, puis remplacez
le `NotImplementedError` par votre solution. Conservez la signature et retournez
le DataFrame demandé : le programme vérifie ses colonnes, types et valeurs.
L'ordre physique des lignes n'est pas garanti et les assertions n'en dépendent
pas. Par exemple :

```bash
exercises/.venv/bin/python exercises/01_selection.py
exercises/.venv/bin/python exercises/solutions/01_selection.py
```

Les autres fichiers portent le même nom dans `exercises/` et `exercises/solutions/`.
Pour vérifier toutes les corrections et la reproductibilité du générateur :

```bash
exercises/.venv/bin/python exercises/test_solutions.py
```

Les corrections sont indépendantes : chaque script démarre et arrête sa session
Spark locale. Ne pas lancer Python avec `-O` : cela désactive les assertions.
Le module streaming crée un checkpoint dédié, et le module production un dossier
Parquet dédié, puis les supprime ; ils refusent de remplacer un dossier préexistant.
La vérification batch Parquet utilise `overwrite` pour être reproductible en local,
mais cela ne garantit pas une publication atomique ni une sémantique exactly-once
sur un stockage de production. Une vraie chaîne doit aussi traiter les rejets,
superviser ses métriques et choisir un stockage et un protocole adaptés.
