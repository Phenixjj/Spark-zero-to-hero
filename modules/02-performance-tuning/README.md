# 02 — Réglage des performances Spark

## Objectifs

Lire un plan physique, identifier les shuffles et les coûts des jointures,
utiliser un broadcast à bon escient et comprendre quand le cache et le
partitionnement aident — ou aggravent — une charge de travail. Les exemples
sont autonomes et utilisent de petits jeux de données en mémoire.

## Exécution

Prérequis : Python **3.11**, Java **17** (ou 11), `JAVA_HOME` configuré si
nécessaire. Dans ce répertoire :

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r ../../requirements.txt
python scripts/demo.py
python -m pytest -q challenges/test_categories.py
```

Les versions sont fixées dans `requirements.txt` à la racine du dépôt,
dont PySpark 3.5.3 ; ces exemples utilisent seulement PySpark et pytest et
PySpark installe aussi py4j. `local[2]` sert à vérifier les principes, pas à mesurer des temps
représentatifs d'un cluster. Les données de la démo ne nécessitent ni serveur
externe ni téléchargement supplémentaire après installation.

## Sous le capot

### Lire le plan

`explain(mode="formatted")` affiche le plan physique. Chercher `Exchange` :
il signale généralement un shuffle ou un échange broadcast. Un
`HashAggregate` peut agréger localement avant un échange puis terminer
l'agrégation après redistribution. Le plan réel dépend des statistiques, de la
version et d'Adaptive Query Execution (AQE) ; un plan avant action n'est pas
forcément le plan final. En production, examiner aussi le plan final et les
métriques dans Spark UI : durée des tâches, volume de shuffle, spill disque et
distribution des durées. Une action déclenche l'exécution ; `explain` seul ne
mesure rien.

### Shuffles et partitions

Un `groupBy` ou une jointure peut redistribuer les données afin de regrouper
les mêmes clés. Cela coûte réseau, sérialisation et disque ; une clé très
fréquente peut créer une tâche lente (**skew**). `repartition(2, "categorie_id")`
dans la démo provoque lui-même un shuffle : ce n'est pas une optimisation
universelle. Choisir le nombre de partitions selon la taille des données et
les ressources ; trop peu limite le parallélisme, trop crée du surcoût.
`coalesce` réduit généralement les partitions sans shuffle complet mais peut
accentuer le déséquilibre. `spark.sql.shuffle.partitions=2` convient à la
démo locale, pas nécessairement à la production. AQE peut adapter le nombre
de partitions et certains choix de jointure.

### Broadcast et réduction du volume

`broadcast(categories)` demande à Spark de distribuer la petite dimension
aux exécuteurs pour éviter un shuffle de la grande table *pour cette
jointure*. Le plan peut contenir `BroadcastHashJoin` et `BroadcastExchange`.
La dimension doit tenir dans la mémoire du driver et des exécuteurs ; un hint
sur une grosse table peut provoquer une panne mémoire. Les types de jointure
limitent aussi le côté qu'on peut construire en broadcast. Spark peut choisir
le broadcast automatiquement selon ses statistiques et son seuil ; le hint ne
remplace pas une mesure.

Projeter les colonnes utiles et filtrer tôt réduit les données manipulées.
Dans le défi, agréger les événements par identifiant avant la jointure réduit
potentiellement le nombre de lignes jointes, mais entraîne un shuffle et
peut être moins avantageux si les clés sont presque toutes distinctes.
Comparer les plans et les métriques sur des données représentatives avant
d'adopter cette stratégie.

### Cache et cycle de vie

`cache()` marque un DataFrame pour persistance, sans le calculer aussitôt.
La démo appelle `count()` pour matérialiser toutes ses partitions, puis
réutilise le DataFrame ; `unpersist()` libère les blocs à la fin. Les blocs
peuvent être évincés ou recalculés. La persistance consomme mémoire et
éventuellement disque ; elle n'est rentable que si la réutilisation compense
son coût. La démo illustre le mécanisme, elle ne prétend pas accélérer un
traitement unique. `collect()` reste réservé aux petits résultats.

## Défi — compter par catégorie

Implémenter `compter_par_categorie(evenements, categories)` dans
`challenges/categories.py`. Les deux DataFrames ont `categorie_id`, la dimension a aussi
`nom` et des identifiants uniques. Compter les événements par clé, puis
joindre la petite dimension en broadcast. Ne retourner que les catégories
connues ayant au moins un événement ; ignorer les clés inconnues et nulles.
Colonnes attendues : `categorie_id`, `nom`, `nb_evenements`, sans ordre imposé.
La dimension est supposée assez petite pour tenir en mémoire.

Une solution est fournie pour comparaison. Les tests couvrent les doublons
d'événements, catégories absentes, clés inconnues et nulles, entrées vides,
et vérifient le choix de broadcast dans le plan physique Spark 3.5. Cette
dernière vérification est volontairement liée au moteur et à la version ;
les tests de résultat restent indépendants de l'ordre.

## Expérimenter

Comparer les plans avec et sans hint, puis avec différents nombres de
partitions. Tester une distribution asymétrique et observer la durée et les
volumes des tâches dans Spark UI sur un environnement adapté. Ne pas conclure
sur la performance à partir des temps des minuscules exemples locaux.
