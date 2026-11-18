# 03 — Ingénierie Spark avancée

Objectif : comprendre et vérifier les choix de plans, de partitionnement, de jointure
et de qualité des données, plutôt que seulement apprendre des appels d'API.
Les exemples utilisent Spark 3.5, Python 3.11 et Java 11 ou 17, en mode local.
Ils ne nécessitent ni cluster ni service externe.

## Installation et exécution

Dans un environnement virtuel Python 3.11, installer les dépendances épinglées
du projet, notamment PySpark, pytest, pandas et PyArrow, puis activer cet
environnement. Depuis ce dossier :

```bash
python -m pip install -r ../../requirements.txt
python scripts/plans_and_windows.py
python scripts/quality_and_storage.py
python scripts/udf_and_arrow.py
python -m pytest -q tests
```

Le driver et les workers doivent utiliser des environnements Python compatibles
contenant pandas et PyArrow. En local, activer le venv ou définir
`PYSPARK_PYTHON` vers son interpréteur ; en cluster, distribuer ces dépendances
aux workers.

Le second exemple crée un répertoire local `.spark-advanced-example`, refuse
de l'écraser s'il existe, puis le supprime à la fin. Les scripts contiennent des
assertions : une assertion qui échoue signifie que la vérification n'a pas passé.
Ne pas lancer Python avec `-O`, qui désactive ces assertions.

## Sous le capot : du DataFrame au travail distribué

Un DataFrame est une description paresseuse. Les transformations construisent un
plan logique ; une action (`collect`, écriture, etc.) déclenche l'analyse des
types, les optimisations Catalyst, puis la construction d'un plan physique.
`explain("formatted")` montre ce dernier : rechercher `Exchange` (shuffle),
`BroadcastExchange`, les filtres poussés et les scans. Le plan réel dépend des
statistiques, de la taille et de la configuration ; ne pas tester son texte comme
une API stable. Une action peut relire ses sources si le résultat n'est pas
persisté. `cache()` ne matérialise rien avant une action ; `unpersist()` libère
les blocs. Le cache occupe mémoire/disque et n'est utile qu'en cas de réutilisation.

Une jointure par clé doit rapprocher les lignes correspondantes. Une jointure
classique peut redistribuer les deux côtés par hash (shuffle, réseau et disque).
Un broadcast copie la petite table sur chaque exécuteur et évite généralement
le shuffle du grand côté, au prix de mémoire et de trafic de diffusion. Le seuil
automatique `spark.sql.autoBroadcastJoinThreshold` est une estimation, pas une
garantie ; un indice `broadcast` ne rend pas une grosse table petite. Les clés
nulles ne correspondent pas avec `=` ; utiliser l'égalité null-safe (`<=>`) si
c'est la règle métier souhaitée. Les doublons de dimension multiplient les lignes :
contrôler l'unicité avant une jointure supposée many-to-one.

Le skew (quelques clés très fréquentes) peut laisser une tâche dominer le temps
total. Examiner les tailles et durées de tâches dans la Spark UI. AQE
(`spark.sql.adaptive.enabled`) peut coalescer les partitions après shuffle,
adapter certaines jointures et traiter certaines partitions skewées. Il dépend des
statistiques d'exécution et de seuils : ce n'est pas une garantie. Saler une clé
peut répartir une clé chaude mais impose de répliquer correctement le petit côté
et d'agréger ensuite ; mesurer avant de l'utiliser.

`repartition(n, "clé")` provoque un shuffle et répartit par clé ;
`coalesce(n)` réduit généralement le nombre de partitions sans redistribution
complète, au risque de déséquilibrer. Les partitions d'exécution ne sont pas
les répertoires `partitionBy` sur disque. Un trop grand nombre de fichiers coûte
cher aux métadonnées ; un nombre trop faible limite le parallélisme. Le filtre sur
une colonne de partition peut éviter de lire certains répertoires (partition
pruning). Parquet est colonnaire : projection et filtres peuvent réduire les
lectures, sous réserve du plan et du connecteur.

Les fenêtres `partitionBy`/`orderBy` regroupent et trient les données. Le tri
peut nécessiter shuffle et mémoire ; pour un classement reproductible, ajouter
un identifiant unique comme briseur d'égalité. `row_number` choisit exactement
une ligne, contrairement à `rank`. Les bornes `rowsBetween` portent sur des
lignes ordonnées, `rangeBetween` sur les valeurs de tri : elles ne sont pas
interchangeables.

## Expressions natives, UDF Python et Pandas UDF

`scripts/udf_and_arrow.py` applique trois fois la même règle (doubler un
`double`, conserver les valeurs nulles) et compare les résultats, y compris
zéro et valeurs négatives. La première version utilise une expression native
Spark, la deuxième une UDF Python scalaire et la troisième une Pandas UDF
scalaire. Le petit jeu de données sert à vérifier la sémantique, pas à mesurer
la performance. Les trois plans sont affichés avec `explain("formatted")`.

Une expression native reste dans le moteur Spark/JVM. Catalyst peut analyser
ses opérateurs et les optimiser ; selon le plan, Spark peut générer du code
pour les exécuter. Préférer les fonctions natives lorsqu'elles expriment la
règle : on évite la frontière JVM/Python et on laisse davantage de possibilités
d'optimisation au moteur.

Une UDF Python ordinaire reçoit une valeur Python à la fois dans un processus
worker Python. Les données traversent la frontière JVM/Python avec conversion
et sérialisation ; le corps de la fonction est opaque à Catalyst. L'exemple
désactive explicitement Arrow pour cette UDF afin d'illustrer le chemin
classique, généralement visible sous `BatchEvalPython`. Spark 3.5 peut aussi
utiliser Arrow pour la sérialisation de certaines UDF Python scalaires ; cela
ne transforme pas leur fonction en opération vectorisée pandas. Une UDF doit
gérer `None` explicitement et ne doit pas dépendre d'effets de bord : une tâche
peut être rejouée ou une expression évaluée plusieurs fois.

Une Pandas UDF scalaire transfère des lots de colonnes via Apache Arrow vers un
worker Python et reçoit des `pandas.Series` ; l'expression pandas opère ici
sur la série entière. Le retour doit contenir autant de lignes que l'entrée du
lot et respecter le type Spark déclaré. `ArrowEvalPython` apparaît généralement
dans son plan physique. `spark.sql.execution.arrow.maxRecordsPerBatch` borne
le nombre de lignes par lot Arrow (l'exemple le fixe à 2) : il ne fixe ni le
nombre de partitions ni une limite exacte de mémoire. Les lots réduisent souvent
le coût par ligne, mais conversion, mémoire JVM et Python, taille des lots et
calcul restant en Python peuvent encore dominer. Un traitement pandas qui boucle
ligne par ligne peut perdre l'intérêt de la vectorisation.

Déclarer un type de retour compatible avec les valeurs produites (`DoubleType`
ici) et tester les conversions : tous les types et structures ne se comportent
pas de façon identique selon les versions de Spark, pandas et PyArrow. Les
représentations pandas des valeurs manquantes varient selon le dtype ; l'exemple
vérifie explicitement le retour SQL `NULL`. Ne pas confondre `NULL` et `NaN`
ni supposer qu'un cast, un fuseau horaire ou une précision décimale conserve
automatiquement la même sémantique entre moteurs. La configuration
`spark.sql.execution.arrow.pyspark.enabled` concerne notamment les conversions
`toPandas`/création depuis pandas ; elle n'est pas nécessaire pour activer les
Pandas UDF. Comparer les plans et mesurer sur des données représentatives avant
de choisir : aucune UDF n'est systématiquement plus rapide qu'une expression
native. Les noms exacts des opérateurs et les optimisations peuvent varier.

## Qualité et exercices

L'exemple qualité sépare lignes valides et rejetées avant d'écrire en Parquet ;
une vérification d'intégrité compare les résultats relus, pas l'ordre des fichiers.
Ne pas utiliser `collect()` pour inspecter une grosse table en production.

Dans `challenges/customer_revenue.py`, implémenter ou modifier :

1. Rejeter les identifiants vides ou absents, dates et montants absents et
   montants négatifs, puis dédupliquer par `event_id`, en gardant la ligne la plus
   récente (égalité départagée par `amount` décroissant puis `customer_id`
   croissant). Un montant égal à zéro est valide. `event_time` doit être un
   timestamp ou une chaîne au format ISO chronologiquement triable.
2. Joindre les clients, conserver les clients sans événement, calculer leur
   revenu et leur rang **par région** (revenu décroissant, `customer_id`
   croissant comme briseur d'égalité). Les événements de clients inconnus sont
   ignorés ; `customer_id` est unique et non nul dans la dimension.

Une implémentation de référence est fournie ; les tests couvrent les doublons,
rejets, clients sans achat et égalités. Modifier l'implémentation puis lancer
`python -m pytest -q tests`.

## Pour aller plus loin

La Spark UI locale expose stages, tâches, shuffle et stockage pendant une
application active. Delta Lake/Iceberg, catalogues distants, stockage objet et
orchestrateurs sont des intégrations optionnelles : installer leurs dépendances
compatibles et configurer leurs services séparément. Parquet local ne fournit
pas à lui seul les transactions ou le contrôle de concurrence d'un format de table.

### Delta Lake optionnel

L'image Docker du projet contient déjà les JAR JVM Delta 3.2.1 dans
`$SPARK_HOME/jars`. Configurer la session **avant sa création**, sans appeler
`configure_spark_with_delta_pip` dans cette image : ce helper configure une
résolution Maven et peut tenter de télécharger les JAR.

```python
from pyspark.sql import SparkSession

builder = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = builder.getOrCreate()
```

En installation pip locale, avec `delta-spark` compatible et accès aux dépôts
Maven, on peut au contraire utiliser le helper pour fournir les JAR :

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

Ces configurations sont facultatives et ne sont pas nécessaires aux scripts
ni aux tests de ce module. Une session Spark déjà créée ne peut pas recevoir
ces extensions rétroactivement.
