# Filière Data Template

Ce repo est un template qui sert à cadrer le développement de pipelines python par l'équipe data. 

## Utilisation comme Template GitHub 
1. Cliquer sur le bouton `Use this template` en haut de ce repo
2. Choisir `Create a new repository`
3. Donner un nom au nouveau projet
4. Cloner le nouveau repo

## Personnalisation du Projet

Après avoir cloné le template, il faut personnaliser le projet :

1. **Renommer le package** : Modifier `name` dans `pyproject.toml`
2. **Mettre à jour la description** : Modifier `description` dans `pyproject.toml`
3. **Renommer le dossier principal** : Renommer `filiere_data_template/` vers le nom du projet
4. **Mettre à jour les imports** : Remplacer `filiere_data_template` par le nom de package dans tous les fichiers Python


## 📁 Structure du Projet

```
data-pipeline-template/
├── filiere_data_template/
│   ├── __init__.py
│   ├── config.py              # Configuration et setup 
│   ├── runner.py              # Script d'orchestration 
│   ├── scripts/               
│   │   ├── __init__.py
│   │   ├── script_1.py        # Script d'exemple 
│   │   └── script_2.py        # Scripts additionnels...
│   ├── sql/
│   │   └── query.sql          # Requêtes SQL
│   └── utils/
│       ├── __init__.py
│       └── base.py            # Classe avec fonctionnalités communes
├── logs/                      # Fichiers de logs 
├── notebooks/                 # Notebooks Jupyter pour l'analyse
├── tests/
│   ├── __init__.py
│   └── test_script_1.py       # Exemples de tests
└── .env                       # Identifiants de connexion
└── .gitignore                 # Fichiers à ne pas partager sur Git 
├── poetry.lock
├── pyproject.toml
├── pytest.ini
└── README.md
└── run.py                     # Script d'exécution 

```

## 🛠️ Installation & Configuration

1. **Installer les dépendances**
   ```bash
   poetry install
   ```

2. **Configurer les variables d'environnement**
   Créer un fichier `.env` à la racine du projet :
   ```python
   # Connexion base de données
   ADDRESS_USER_DEV=mysql://user:password@192.168.20.92:33068
   ADDRESS_USER_DEV=mysql://user:password@192.168.20.91:33068
   ADRESS_USER_PROD=mysql://user:password@sql-prod.carbone4h.com:33067
   ```
   remplacer `user` et `password` avec les identifiants de connexion à la base de données

3. **Mettre à jour la configuration**
   Modifier `filiere_data_template/config.py` pour correspondre à ton environnement :
   ```python
   ENV = "DEV"  # ou "PREPROD", "PROD"
   DB_PROD = "ta_base_production"
   ```

##  Utilisation

### Exécuter un Script

```bash
# Exécuter un script spécifique
poetry run python run.py script_1

# Exécuter le pipeline entier
poetry run python run.py all
```

### Créer un Nouveau Script

1. **Créer un nouveau fichier de script** dans `filiere_data_template/scripts/`

2. **Ajouter les requête SQL** dans `filiere_data_template/sql/`

3. **Créer des tests** dans `tests/`

4. **Ajouter les nouveaux packages python importés**  
```bash
poetry add <package_name> # e.g poetry add pandas
```

## Tests

### Exécuter les Tests

```bash
# Exécuter tous les tests
poetry run pytest

# Exécuter un fichier de test spécifique
poetry run pytest tests/test_script_1.py
```
**NB :** Dans l'exemple fourni (`script_1`) l'exécution de tests est incluse au run du script.

## Fichiers log

Chaque script crée automatiquement un fichier de log avec le même nom dans le répertoire `logs/` :


### Fichier de Configuration

Paramètres clés dans `filiere_data_template/config.py` :

```python
ENV = "DEV"                    # Environnement (DEV/PREPROD/PROD)
DB_STAGING = "staging"         # Nom de la base de staging
DB_PROD = "production"         # Nom de la base de production
LOG_LEVEL = "INFO"             # Niveau de log pour l'affichage
```

## Bonnes Pratiques

1. **Hériter de BaseClass** : Tous les scripts doivent hériter de `BaseClass` qui contients les paramètres de connexions à la bdd et des méthodes communes à différents scripts.
2. **Suivre le pattern ETL** : Extract → Transform → Load
3. **Ajouter des logs** : Logger les étapes importantes
4. **Écrire des tests** : Inclure des tests unitaires pour tester le comportement des fonctions et d'intégration pour tester les données transformées avant l'intégration à la bdd.
5. **Utiliser les branches et les tags sur git** : Utiliser des branches séparées pour les fonctionalités développées et les tags git pour historiser les différentes versions du code.





