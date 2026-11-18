# 01 — Fondations de Spark

## Objectifs

Comprendre comment un programme PySpark devient un calcul distribué, manipuler
des DataFrames typés et raisonner sur les données manquantes, les jointures et
les agrégations. Les exemples sont autonomes : aucun cluster ni fichier de
données n'est nécessaire.

## Exécution

Prérequis : Python **3.11**, Java **17** (ou 11), `JAVA_HOME` configuré si
nécessaire. Dans ce répertoire :

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r ../../requirements.txt
python scripts/demo.py
python -m pytest -q challenges/test_ventes.py
```

Le premier téléchargement de PySpark est volumineux. `local[2]` utilise deux
threads locaux : le résultat illustre l'API distribuée, pas la performance
d'un cluster. Les versions des dépendances sont fixées dans le
`requirements.txt` à la racine du dépôt, dont PySpark 3.5.3 ; py4j est installé
comme dépendance de PySpark. Ces exemples utilisent seulement PySpark et pytest.

## Sous le capot

### Driver, exécuteurs et partitions

La `SparkSession` crée le point d'entrée SQL et un `SparkContext`. Le processus
Python du **driver** construit le programme ; la JVM du driver planifie les
tâches, et les exécuteurs JVM traitent des **partitions**. En mode local ils
partagent la JVM locale, même si les mêmes concepts de tâches s'appliquent.
Des workers Python interviennent notamment pour les UDF Python ; les fonctions
SQL natives utilisées ici s'exécutent côté moteur Spark.

Un DataFrame est une description distribuée, pas une liste Python. Son schéma
décrit types et nullabilité ; celui de `commandes` est explicite dans
`scripts/demo.py`. L'inférence est pratique pour les données locales mais fragile avec
des données vides, toutes nulles ou des types ambigus. En production, choisir
un schéma et des types de précision adaptés : `double` convient à cette
démonstration, tandis que `DecimalType` est préférable pour l'argent.

### Évaluation paresseuse et plans

`filter`, `select`, `join` et `groupBy().agg()` construisent des
**transformations** ; elles ne lisent pas toutes les données immédiatement.
`collect`, `count`, `show` et les écritures sont des **actions** qui déclenchent
un job. `explain()` demande une représentation du plan sans collecter les
résultats. Catalyst analyse les noms et types, optimise le plan logique, puis
Spark choisit un plan physique. Plusieurs actions peuvent recalculer la même
lignée si elle n'est pas persistée.

Dans la démo, `montant > 0` élimine aussi les valeurs nulles : la comparaison
avec `NULL` est inconnue et le filtre ne garde que `true`. Utiliser `isNull()`
ou `coalesce` quand un autre traitement est voulu. La jointure interne ne
conserve que les identifiants présents des deux côtés. L'agrégation par ville
nécessite généralement un **shuffle** : Spark redistribue les lignes par clé,
ce qui implique sérialisation, réseau et éventuellement disque en cluster.
L'ordre des lignes résultantes n'est pas garanti ; ici les assertions
comparent des dictionnaires, pas l'ordre de `collect()`.

`collect()` ramène toutes les lignes au driver : réserver cette méthode aux
petits résultats. Sur de grandes données, écrire un résultat distribué ou
examiner un échantillon borné. Consulter `explain("formatted")` et l'interface
Spark UI (désactivée dans les exemples) pour relier plans, étapes et tâches.

## Défi — ventes par client

Implémenter `ventes_par_client(commandes, clients)` dans `challenges/ventes.py` :

* `commandes` contient `client_id` et `montant` ; `clients` contient
  `client_id` et `nom`, avec un identifiant unique par client.
* Ignorer les montants nuls, nuls numériques ou négatifs et les commandes
  dont le client est inconnu. Compter les lignes restantes et sommer leurs
  montants par client.
* Garder **tous** les clients, même sans vente valide, avec un compte et un
  total à zéro. Retourner `client_id`, `nom`, `nb_commandes`, `total`, sans
  imposer d'ordre.

Une solution exécutable est fournie : essayez de la réécrire avant de la
consulter. `challenges/test_ventes.py` vérifie les cas nominaux, les valeurs nulles,
les clés inconnues, les entrées vides et les colonnes. Les tests comparent les
valeurs et non le plan, afin de laisser plusieurs implémentations possibles.

## Pour aller plus loin

Modifier une jointure interne en jointure gauche et comparer les lignes
conservées ; inspecter `explain("extended")` avant et après un filtre. Comparer
`count("*")` à `count("montant")` en présence de nulls. Ces observations
dépendent des données et de la version de Spark.
