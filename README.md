# Spark Zero-to-Hero

Un parcours pratique, en français, pour passer des DataFrames à l'exploitation de
jobs Apache Spark en production. Les exemples utilisent PySpark ; les différences
avec les API Scala/Java sont expliquées lorsque leur modèle est différent.

## Parcours

1. [Fondations](modules/01-fundations/) — architecture, plans et Spark UI.
2. [Performance](modules/02-performance-tuning/) — jointures, partitions et mémoire.
3. [Ingénierie avancée](modules/03-advanced-engineering/) — fenêtres, Arrow et Delta.
4. [Streaming](modules/04-streaming/) — état, watermarks et Kafka.
5. [Production et MLOps](modules/05-%20MLOps-and-Production/) — tests, livraison et observabilité.

Chaque module contient un cours, des scripts et un mini-projet avec tests.
Les [exercices et leurs corrections](exercises/) complètent ce parcours ; leurs
données sont générées localement. Commencer par les fondations puis suivre
l'ordre des modules.

## Prérequis

Python 3.11, Java 17, une connaissance de Python et SQL, ou Docker avec Compose.
Prévoir au moins 4 Go de mémoire disponibles. Spark est distribué : un exemple
local ne remplace pas une mesure sur les données et le cluster de production.

## Démarrage avec Docker

Depuis la racine du dépôt :

```bash
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
docker compose -f docker/compose.yaml up --build -d
docker compose -f docker/compose.yaml logs jupyter
```

Ouvrir l'URL avec jeton affichée dans les logs à `http://127.0.0.1:8888`.
Le répertoire `/workspace` correspond au dépôt. Le master Spark est visible à
`http://127.0.0.1:8080` et le worker à `http://127.0.0.1:8081`. L'interface
du driver est publiée sur `127.0.0.1:4040` (modifier avec `SPARK_UI_PORT` en cas
de conflit).
Les ports sont limités à la machine locale ; ces services ne sont pas sécurisés
pour une exposition publique.
Les variables `LOCAL_UID` et `LOCAL_GID` permettent à l'utilisateur non-root
du conteneur d'écrire dans le dépôt monté. Sur les plateformes sans `id`, elles
valent par défaut 1000 ; adapter ces valeurs si nécessaire. Construire depuis
un compte non-root.

Les scripts utilisent par défaut `local[*]` ou le master configuré par Spark.
Pour exécuter un notebook sur le cluster Compose, configurer **avant** de créer
la première session :

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("spark://spark-master:7077")
         .config("spark.driver.host", "jupyter").getOrCreate())
```

Les fichiers du dépôt sont montés au même chemin sur tous les conteneurs. Les
processus driver et executors ont ainsi accès aux mêmes données d'exercice ;
ce montage partagé n'est pas un stockage distribué de production. L'image inclut
les JAR Delta Lake 3.2.1 compatibles avec Spark 3.5.3 ; pour utiliser une table
Delta locale, configurer la session **avant sa création** :

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[2]")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.range(3).write.format("delta").mode("overwrite").save("/tmp/demo-delta")
assert spark.read.format("delta").load("/tmp/demo-delta").count() == 3
```

Le chemin `/tmp` est éphémère. Pour conserver une table, choisir un répertoire
accessible en écriture dans `/workspace`. Arrêter les services avec
`docker compose -f docker/compose.yaml down`.

## Installation locale sans Docker

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m jupyterlab
```

Java 17 doit être installé et `JAVA_HOME` configuré si nécessaire. Pour lancer
un script, se placer à la racine du dépôt et suivre les commandes de son module.
Les tests utilisent `python -m pytest` ; chaque mini-projet précise sa commande.
Hors Docker, le connecteur Delta et les dépendances Kafka nécessitent des
artefacts JVM supplémentaires ; consulter les modules correspondants.

## Sécurité et limites

L'environnement Compose est réservé à l'apprentissage local, pas à la
production : Spark standalone n'y active pas l'authentification ni le chiffrement.
Ne pas y placer de données sensibles. En production, utiliser les politiques
d'accès, la gestion de secrets, le chiffrement et la supervision de votre
plateforme ; mesurer les plans et les métriques sur des charges représentatives.
