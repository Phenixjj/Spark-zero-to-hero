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

```text
Programme Python (driver) -> SparkSession / plan SQL (JVM du driver)
                                      |
                           planification des tâches
                                      v
                     exécuteurs JVM (localement ou sur un cluster)
                          |                 |
                    partitions        workers Python si nécessaire
                          |
                  lecture / shuffle / écriture
```

Le gestionnaire de cluster attribue les ressources en mode distribué ; le
driver coordonne les exécuteurs. Le mode local regroupe les composants dans
un seul environnement et ne reproduit pas les coûts réseau d'un cluster.

### RDD, DataFrame et Dataset

* Un **RDD** est une collection distribuée de partitions, manipulée par des
  opérations de bas niveau comme `map` et `reduceByKey`. En PySpark, les
  fonctions Python passent par des workers et une frontière de sérialisation ;
  Spark SQL ne peut pas optimiser leur logique arbitraire comme une expression
  SQL. Les RDD restent utiles pour des traitements non structurés ou un contrôle
  fin des partitions.
* Un **DataFrame** possède des colonnes et un schéma ; les expressions SQL
  natives sont analysées et optimisées par Catalyst. C'est l'API conseillée
  pour la plupart des données structurées en PySpark.
* En **Scala et Java**, un `Dataset[T]` offre une API typée, avec des encodeurs
  entre objets JVM et représentation interne. Un DataFrame est un
  `Dataset[Row]` non typé au niveau de chaque champ. PySpark expose les
  DataFrames, pas l'API de Dataset typé `Dataset[T]` de Scala/Java.

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

### Tungsten : mémoire et exécution

Catalyst analyse et optimise les requêtes puis choisit un plan physique ;
**Tungsten** désigne des techniques d'exécution utilisées par Spark SQL pour
réduire les coûts CPU et mémoire. Par exemple, `UnsafeRow` représente certaines
lignes sous une forme binaire compacte, avec des champs fixes et des offsets
vers les valeurs variables, plutôt que comme une multitude d'objets Java.
Spark gère des régions mémoire sur le tas JVM (*on-heap*) et peut utiliser de
la mémoire hors tas (*off-heap*) lorsqu'elle est configurée. Cela peut réduire
la pression sur le ramasse-miettes, sans supprimer les limites mémoire ni les
écritures temporaires sur disque (*spill*).

Le **whole-stage code generation** fusionne certains opérateurs physiques
compatibles et génère du code Java pour traiter les lignes avec moins d'appels
et d'objets intermédiaires. Dans `explain("formatted")`, les identifiants
`codegen id` regroupent les opérateurs concernés ; `explain("codegen")` montre
les sous-arbres et le code généré disponibles. Tous les opérateurs ne sont pas
fusionnables : les échanges, certaines opérations et les UDF Python peuvent
interrompre une étape. Avec AQE, le plan initial peut différer du plan final
après une action ; inspecter également ce dernier dans Spark UI. La forme
exacte dépend de la requête, de la configuration et de la version de Spark.

### Lire la Spark UI

Pour explorer l'interface, supprimer ou modifier la configuration
`spark.ui.enabled=false` de la session de la démo, puis ouvrir l'URL affichée
par Spark (généralement `http://localhost:4040`) pendant que l'application
fonctionne. Une application courte peut terminer avant l'inspection ; en
cluster, suivre le lien fourni par le gestionnaire ou le serveur d'historique
si celui-ci est configuré.

* **Jobs** : repérer le job créé par une action, sa durée et les étapes
  associées ; plusieurs actions peuvent produire plusieurs jobs.
* **Stages** : repérer les frontières de shuffle, les données lues et écrites,
  les erreurs et les étapes lentes. Une étape comprend des tâches exécutables
  en parallèle ; une partition donne généralement lieu à une tâche dans
  l'étape correspondante, mais reprises et exécution spéculative peuvent
  produire plusieurs tentatives.
* **Tasks** : comparer durées, volumes, temps GC et spill entre tâches d'une
  même étape. Une tâche beaucoup plus lente ou volumineuse peut signaler un
  déséquilibre de données ; vérifier les métriques avant de conclure.
* **SQL / DataFrame** : relier une action à son plan physique et aux métriques
  des opérateurs, notamment les échanges et le nombre de lignes ; avec AQE,
  examiner le plan final après exécution.
* **Storage** : voir les RDD et DataFrames persistés, leurs partitions
  matérialisées et leur occupation mémoire/disque. `cache()` seul ne remplit
  pas le cache : une action doit d'abord calculer les partitions.

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
