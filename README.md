# Netfloox_Project
Recommandation of movies by a model of artificial intelligence

# Contexte et objectif du projet
Création d'un système de recommandation de films

## Création de l'environnement virtuel sous Windows et installation des librairies et dépendances nécéssaires au bon fonctionnement du Projet 
```shell
  # Création de l'environnement virtuel
  python -m venv venv
  # Activation de l'environnement virtuel
  .\venv\Scripts\activate
  # Installation des librairies listées dans 'requirements.txt'
  pip install -r requirements.txt
  # Affichage des librairies installées dans l'environnement virtuel
  pip freeze
```

## Lancer l'application Streamlit
```shell
  streamlit run streamlit_app.py
```

## Source des données
https://datasets.imdbws.com


docker build -t netflooxproject .


