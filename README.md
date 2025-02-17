R# Netfloox

Projet Netfloox

## Installation / config

```bash
 python3 -m venv ENV
 source ENV/bin/activate 
 pip install -r requirements.txt
```

Utilisation de fichier environnement (`variable_env/VAR.env`) 
```text
PATH_DONNEES = "../.."
NOM_BASES_DONNEES = "postgres"
USERAZURE = "psqladmin"
PASSWORD ="*******"
HOST = "netfloox-psqlflexibleserver-1.postgres.database.azure.com"
PORT = "5432"
```
# Application : netfloox.py
Lancement : streamlit run netfloox.py


## Notebook : recommandation.ipynb

Contient le cheminement, les tests pour le modèle de recommandation

## Les fichiers python

01.create_schema.py : Création des tables et des clefs de la database

02.importation_data.py : Importation des données depuis les fichiers .tsv

03.extraction_features_popularite.py : Extraction des données (pour la popularité)depuis la database et retourne un DataFrame

04.extraction_features_recommandation.py : Extraction des features utiles au système de recommandation

05.recommandation_cosine.py : Système de recommandation à partir d'un titre de film.

06.create_view : Creation d'une Vue matérialisée

fct_extract.py : Librairie de la fonction extraire_donnees_BDD() utilisée par 05.recommandation_cosine.py

## Les fichiers sql

creation_tables.sql : Requete de cration des tables de la BDD
creation_clefs.sql : comme son nom l'indique.
