# 04 — Structured Streaming

Objectif : construire un traitement incrémental, comprendre l'état, les
watermarks et la reprise, et tester localement sans Kafka, cloud ni serveur.
Prérequis : Spark 3.5.x, Python 3.11 et Java 11 ou 17. Installer par exemple
`pyspark==3.5.3` et `pytest` dans un environnement virtuel. Depuis ce dossier :

```bash
python scripts/file_stream.py
python -m pytest -q tests
```

Le script crée `.spark-streaming-example` dans le répertoire courant, refuse
de l'écraser et le supprime après exécution. Les tests créent et nettoient leurs
propres répertoires dans ce module. Ne pas utiliser `python -O` : les scripts
contiennent des assertions.

## Sous le capot

Un DataFrame streaming est un plan continu, pas une collection qu'on peut
`collect()` directement. Au démarrage, le moteur micro-batch découvre de nouvelles
plages de données, exécute un plan Spark pour chaque lot et enregistre offsets,
commits et éventuellement état dans le checkpoint. `availableNow=True` traite
les données disponibles puis s'arrête ; le déclencheur par défaut attend
indéfiniment, et un intervalle fixe règle la fréquence, pas une garantie de
latence. `processAllAvailable()` est pratique dans un test local, mais peut
attendre indéfiniment si la source continue à recevoir des données.

La source fichiers exige un schéma explicite. Chaque fichier doit apparaître
entièrement écrit (publication atomique) et ne doit pas être modifié après
découverte. `maxFilesPerTrigger` limite les fichiers par lot. Une source réelle
peut présenter d'autres garanties de rétention et d'offsets.

L'event time est la date de l'événement ; le processing time est l'heure à
laquelle Spark le traite. Un watermark de 10 minutes est calculé à partir du
maximum d'event time observé moins 10 minutes, de manière coordonnée entre lots.
Il permet l'éviction d'état et, pour une agrégation fenêtrée en mode append,
l'émission des fenêtres suffisamment anciennes. Il n'est ni une horloge murale
ni une minuterie : sans nouveaux événements, une fenêtre peut rester ouverte.
Les données arrivant dans le délai sont prises en compte ; au-delà, leur prise
en compte n'est pas garantie. La progression du watermark et l'éviction
s'effectuent entre micro-batches, pas immédiatement à chaque ligne.

Une fenêtre temporelle de 5 minutes produit des bornes `[début, fin)` en UTC
avec la configuration des exemples. `update` émet les agrégats modifiés,
`complete` réémet toute la table de résultats, et `append` n'émet que les lignes
finales lorsqu'une opération à état le permet. Tous les sinks n'acceptent pas
tous les modes ; par exemple, le sink fichiers utilise append. Une déduplication
streaming sans watermark peut conserver un état non borné.
`dropDuplicatesWithinWatermark` (Spark 3.5) déduplique par identifiant à
l'intérieur de l'horizon du watermark, à condition de choisir un délai supérieur
à l'écart d'event time entre copies. Après éviction, ne pas supposer une
déduplication globale éternelle.

Le checkpoint contient la progression et l'état : le conserver sur un stockage
durable en production, avec un chemin distinct par requête. La reprise n'est
pas une autorisation à changer arbitrairement source, schéma, clés d'état ou sink.
Le sink fichiers avec checkpoint peut éviter de réécrire les lots déjà validés ;
cela ne garantit pas exactement une fois pour n'importe quel système externe.
`foreachBatch` est au moins une fois par défaut : rendre les écritures idempotentes
en utilisant `batchId` **et** une transaction/contrainte adaptée au système cible.
Ne pas effacer un checkpoint pour « réparer » sans plan de reprise : cela peut
rejouer la source.

## Exemple et défis

`scripts/file_stream.py` écrit des fichiers JSON locaux, lance une requête
`availableNow`, puis la relance avec le même checkpoint après ajout d'un fichier.
Il vérifie que les anciennes lignes ne sont pas rejouées dans la sortie Parquet.

Dans `challenges/events.py`, une solution de référence réalise ces défis :

1. Convertir `event_time` en timestamp, nettoyer `event_id` et `category`,
   rejeter chaînes vides, dates invalides, valeurs nulles et valeurs négatives
   (zéro est accepté).
2. Dédupliquer les identifiants avec un watermark de 10 minutes.
3. Compter les événements par catégorie et fenêtre de 5 minutes. Les colonnes
   retournées sont `window`, `category`, `count`.

Les tests vérifient les rejets, le comptage et la reprise d'un état de
déduplication entre deux exécutions. Ils utilisent des fichiers et un sink
mémoire local, pas de sleeps ni d'infrastructure externe.

Le test de comptage utilise le mode `complete` et vérifie cette chaîne de deux
opérations à état sur plusieurs micro-batches locaux. Ce résultat ne garantit
pas l'absence de perte de données tardives dans tous les cas : l'opérateur en
aval peut rejeter des lignes émises tardivement par celui en amont selon le
watermark global. Le mode, le sink et les opérations prises en charge imposent
des contraintes supplémentaires. Pour une application de production qui
exige de maîtriser ce risque, séparer les étapes en requêtes avec sorties
durables et checkpoints distincts, puis tester explicitement les retards et
les reprises.

## Intégrations optionnelles et exploitation

### Exemple Kafka et JSON

Cet exemple facultatif nécessite un broker Kafka accessible, un topic existant
et le connecteur Spark SQL Kafka correspondant à la version Spark **et Scala**
(Spark 3.5.3 précompilé pour Scala 2.12 dans la commande ci-dessous). `pyspark`
seul ne fournit pas le connecteur. Avec accès Maven, depuis ce dossier :

```bash
spark-submit --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  scripts/kafka_stream.py --bootstrap-servers localhost:9092 \
  --topic events --checkpoint ./checkpoint-kafka-demo
```

Le téléchargement Maven est nécessaire à la première résolution sauf si les
JAR compatibles et leurs dépendances sont déjà installés. Adapter la version du
connecteur à celle de Spark ; le broker et le topic doivent être préparés
séparément. Envoyer par exemple comme valeur Kafka UTF-8 :

```json
{"event_id":"e1","category":"achat","event_time":"2024-01-01 12:00:00","value":10.5}
```

Le script définit le schéma `event_id STRING, category STRING, event_time
STRING, value DOUBLE`, lit avec `readStream.format("kafka")`, décode `value`
avec `from_json`, convertit le timestamp en UTC, filtre les lignes invalides et
affiche les résultats et coordonnées Kafka (`topic`, `partition`, `offset`)
sur la console. L'arrêter avec Ctrl-C ; relancer la même commande reprend depuis
le checkpoint. Le filtre est pédagogique : en production, compter et envoyer les
rejets vers un stockage adapté plutôt que les perdre silencieusement.

`startingOffsets=earliest` ne s'applique qu'au démarrage d'une nouvelle requête ;
un checkpoint existant détermine la reprise. La rétention Kafka doit couvrir
les interruptions ; si des offsets ont disparu, le comportement par défaut
`failOnDataLoss=true` signale la perte possible au lieu de l'ignorer. Le sink
console est destiné au diagnostic, pas à une livraison fiable ou exactement
une fois : un lot peut être réaffiché après reprise. Pour un sink externe avec
`foreachBatch`, prévoir une écriture idempotente et transactionnelle par
`batchId` ; le traitement est au moins une fois par défaut. Sécuriser le broker
(TLS/SASL, contrôle d'accès et secrets hors du code) et configurer les options
Kafka correspondantes pour tout déploiement réel. Le test du décodeur utilise
un DataFrame local et ne contacte pas de broker.

Object stores, catalogues et formats transactionnels nécessitent aussi leurs
connecteurs et services. Surveiller `lastProgress`, les offsets, le retard, les
lignes rejetées, la taille de l'état et les durées des lots ; dimensionner le
checkpoint et la rétention de la source selon le temps maximal de reprise.

Pour Delta dans l'image Docker, les JAR 3.2.1 sont déjà dans `$SPARK_HOME/jars` :
configurer directement les extensions et le catalogue Spark, sans
`configure_spark_with_delta_pip` qui cherche à télécharger les JAR. En
installation pip locale, le helper peut être utilisé. Voir la
[configuration Delta](../03-advanced-engineering/README.md#delta-lake-optionnel).
