# Importation des librairies

import pandas as pd
import numpy as np
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text



load_dotenv(r'variable_env/VAR.env')
nom_base_de_donnees = os.getenv("NOM_BASES_DONNEES")
utilisateur = os.getenv("USERAZURE")
mot_de_passe = os.getenv("PASSWORD")   
host = os.getenv("HOST")
port = os.getenv("PORT")
path_donnees = os.getenv("PATH_DONNEES")

# URL de connexion PostgreSQL
DATABASE_URL = f"postgresql://{utilisateur}:{mot_de_passe}@{host}:{port}/{nom_base_de_donnees}"
# Créer un moteur SQLAlchemy (engine)
engine = create_engine(DATABASE_URL)

SQL = text("""
CREATE MATERIALIZED VIEW IF NOT EXISTS netfloox.v_roles_mat AS
WITH tb_roles AS (
    SELECT 
        tconst,
        STRING_AGG(nconst, ',') FILTER (WHERE category = 'director') AS director,
        STRING_AGG(nconst, ',') FILTER (WHERE category = 'actress') AS actress,
        STRING_AGG(nconst, ',') FILTER (WHERE category = 'producer') AS producer,
        STRING_AGG(nconst, ',') FILTER (WHERE category = 'actor') AS actor
    FROM netfloox.title_principals
    GROUP BY tconst
)
SELECT * FROM tb_roles;
""")




# Exécution de la création de la vue matérialisée
try:
    with engine.begin() as connection:
        print("Connexion réussie à PostgreSQL avec SQLAlchemy !")
        connection.execute(SQL)
        print("Requête SQL exécutée !")
        connection.commit()  # Nécessaire pour valider la transaction
        print("Commit done !!")
except Exception as e:
    print("Erreur de connexion :", e)

connection.close()