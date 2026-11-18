# 05 — MLOps et mise en production avec Spark

## Objectifs

- Tester des transformations PySpark avec **pytest** et **chispa** sans dépendre d'un cluster distant.
- Automatiser les contrôles, puis construire et déployer un artefact versionné.
- Observer les traitements en production et réagir aux échecs ou à la dérive des données.

Prérequis : DataFrames, schémas, filtres, agrégations et notions de Git. Cible : **Python 3.11, Spark/PySpark 3.5.3 et Java 17**. Ce module utilise le mode local ; en production, le client et le cluster doivent utiliser des versions Spark et Python compatibles.

## Démarrage

Depuis la racine du dépôt (le nom du dossier contient un espace) :

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cd "modules/05- MLOps-and-Production"
python scripts/run_pipeline.py
python -m pytest -q
```

Il faut un JDK disponible via `JAVA_HOME` ou `PATH`. Sur Windows, activer l'environnement avec `.venv\Scripts\activate`. Le script construit ses données en mémoire, vérifie le nombre de ventes valides et les résultats, puis arrête Spark ; aucune donnée externe n'est nécessaire. Résultat attendu : `café = 10.5`, `thé = 4.0`, puis `Assertions réussies.` Les `assert` du script sont pédagogiques : ne pas lancer Python avec `-O` et ne pas les utiliser comme seul contrôle de qualité en production.

Le contrat de `scripts/pipeline.py` est explicite : schéma `produit string, quantite double, prix_unitaire double` ; produit normalisé par suppression des espaces en bordure et passage en minuscules ; exclusion des produits vides ou nuls, quantités non finies, nulles ou négatives, et prix non finis, nuls au sens SQL ou négatifs. Un prix zéro est accepté. Le chiffre d'affaires est la somme de `quantite * prix_unitaire`, arrondie **après** agrégation à deux décimales. L'ordre des lignes n'est pas garanti. Ce jeu utilise des `double` pour faciliter la démonstration ; pour des montants réels, utiliser des décimales, définir précisément les règles d'arrondi et contrôler les dépassements.

## Théorie : tester un pipeline Spark

### Concevoir des transformations testables

Séparer les fonctions de transformation (DataFrame en entrée et en sortie) des effets de bord : création de session, lecture, écriture et lancement du job. Les premières se testent avec de petits DataFrames en mémoire ; les secondes nécessitent des tests d'intégration sur des fichiers ou services isolés. Spark est paresseux : une transformation seule ne prouve rien, il faut déclencher une action (`collect`, `count`, écriture) pour exécuter le plan. Limiter `collect` aux petits jeux de test, jamais à des données de production non bornées.

Une stratégie utile :

1. **Unitaires** : normalisation, filtres, agrégations, valeurs limites, schéma et `null`. Tester les lignes invalides, les doublons, les jeux vides, les dates/fuseaux ou NaN lorsque pertinents.
2. **Intégration** : lecture/écriture réelle dans un répertoire isolé, schémas et partitions, puis relire et comparer. Ajouter des tests sur les connecteurs et versions utilisés en production.
3. **Contrats et bout en bout** : vérifier les colonnes, types, taux de nulls, règles métier et comportement sur un environnement proche de production. Les petits tests locaux ne prouvent ni la performance ni l'absence de problèmes distribués.

Le fichier `conftest.py` crée une session `local[2]` partagée pendant les tests et la ferme ensuite. Les tests emploient des schémas explicites, notamment pour l'entrée vide, et des données déterministes. Éviter les horloges, sources réseau et configurations globales non maîtrisées. `pytest` découvre les fichiers `test_*.py`, injecte les fixtures par nom et affiche les échecs ; `python -m pytest -q` exécute la suite.

Avec chispa, `assert_df_equality` compare les données et le schéma, tandis que `assert_approx_df_equality` accepte une précision numérique. Spark ne garantit pas l'ordre après un shuffle : `ignore_row_order=True` évite un test fragile. Ne pas ignorer systématiquement la nullabilité ou l'ordre des colonnes : ce sont parfois des éléments du contrat. Ici, `ignore_nullable=True` est limité aux résultats agrégés dont la nullabilité inférée diffère de celle du schéma attendu. Pour les flottants, choisir une tolérance justifiée ; pour les décimales monétaires, préférer une comparaison exacte. Les erreurs chispa présentent les différences entre DataFrames.

## CI/CD : de la modification au déploiement

**CI** : à chaque branche ou pull request, installer des versions maîtrisées, vérifier le code et exécuter les tests. Conserver les journaux et résultats, rendre les contrôles obligatoires avant fusion. **CD** : après validation, produire un artefact immuable, le promouvoir entre environnements et ne déployer que la version testée. Un exemple de workflow GitHub Actions à adapter (illustration uniquement ; aucun workflow n'est installé par ce module) :

```yaml
name: spark-tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Installer et tester
        run: |
          python -m pip install -r requirements.txt
          cd "modules/05- MLOps-and-Production"
          python scripts/run_pipeline.py
          python -m pytest -q
```

En pratique, épingler aussi les actions à des SHA vérifiés, appliquer le moindre privilège aux permissions, analyser dépendances et secrets, et ajouter les tests d'intégration adaptés. Les données de test ne doivent pas contenir de secrets ou de données personnelles. Garder les identifiants dans un gestionnaire de secrets, jamais dans l'artefact ni les logs.

Pour CD, publier le paquet ou l'image avec une version et l'identifiant du commit, déployer d'abord en préproduction, exécuter des contrôles sur des données représentatives, puis promouvoir ce même artefact. Prévoir approbation si nécessaire, exécution idempotente, migrations compatibles, stratégie de reprise et rollback documenté. Un rollback du code ne restaure pas automatiquement des données déjà écrites : gérer séparément les versions et la restauration des données.

## Packaging et exploitation

- Construire un paquet Python versionné (wheel) avec ses modules et dépendances déclarées, ou une image conteneur reproductible. Épingler les versions et conserver un inventaire des dépendances ; séparer dépendances d'exécution et de test dans un vrai projet. Le `requirements.txt` à la racine inclut les deux pour simplifier le démarrage.
- Distribuer l'artefact à tous les exécuteurs avec le mécanisme de l'environnement cible : installation dans l'image/environnement des workers ou distribution de code Python via `spark-submit --py-files` pour des archives compatibles. `--py-files` ne résout pas à lui seul les dépendances natives ou les environnements Python. Vérifier les versions de Python, PySpark, Java et connecteurs sur driver et workers.
- Configurer chemins, environnement, ressources et paramètres via arguments ou configuration externe validée, sans secrets en dur. Distinguer erreurs de données, erreurs transitoires et erreurs de code ; ne retenter automatiquement que les opérations sûres et idempotentes.
- Suivre durée, statut, volume lu/écrit, nombre et taux de lignes rejetées, fraîcheur, doublons et distribution des valeurs. Comparer à des seuils métier et à une référence historique pour détecter les dérives. Émettre des métriques et logs structurés avec identifiant de traitement et version, sans données sensibles.
- Utiliser la Spark UI et les journaux du cluster pour examiner tâches lentes, skew, shuffle, mémoire et échecs d'exécuteurs ; les exporter vers les outils de supervision de l'organisation. Définir alertes exploitables, responsable, runbook, tableaux de bord et objectifs de service. Tester les alertes et exercices de reprise, pas seulement le chemin nominal.

## Défi

Consultez [qualité et traçabilité des ventes](challenges/README.md) et son test de départ exécutable.
