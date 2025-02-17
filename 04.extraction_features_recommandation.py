# Importation des librairies

import pandas as pd
import numpy as np
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine



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

# connexion à la BDD
try:
    with engine.connect() as connection:
        print("Connexion réussie à PostgreSQL avec SQLAlchemy !")
except Exception as e:
    print("Erreur de connexion :", e)

SQL = '''
SET search_path TO echantillon_test; -- A renouveller à chaque requête SQL

WITH tb_roles AS (
--SET search_path TO echantillon_test; -- A renouveller à chaque requête SQL
SELECT tconst
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'self') AS self
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'writer') AS writer
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'editor') AS editor
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'composer') AS composer
--, STRING_AGG(nconst, ',') FILTER (WHERE category = 'casting_director') AS casting_director
--, STRING_AGG(nconst, ',') FILTER (WHERE category = 'production_designer') AS production_designer
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'cinematographer') AS cinematographer
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'director') AS director
--, STRING_AGG(nconst, ',') FILTER (WHERE category = 'archive_footage') AS archive_footage
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'actress') AS actress
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'producer') AS producer
, STRING_AGG(nconst, ',') FILTER (WHERE category = 'actor') AS actor
FROM title_principals
GROUP BY tconst
ORDER BY tconst
),

tb_regions AS (
SELECT "titleId"
, STRING_AGG(region, ',') AS region
FROM title_akas
GROUP BY "titleId"
)

SELECT tb_film.tconst
, "primaryTitle"
, "titleType"
, "isAdult"
, "startYear"
, "genres"

-- Dats from title_ratings
, "averageRating"
, "numVotes"

-- Datas from  title_crew
--, split_part("directors", ',', 1) AS id_director_1
--, split_part("directors", ',', 2) AS id_director_2
--, split_part("directors", ',', 3) AS id_director_3
--, split_part("writers", ',', 1) AS id_writer_1
--, split_part("writers", ',', 2) AS id_writer_2
--, split_part("writers", ',', 3) AS id_writer_3
-- Datas from tb_langages

-- Datas from tb_regions
, region

-- Datas from tb_roles
, self
, writer
, editor
, composer
, cinematographer
, director
, actress
, producer
, actor

FROM title_basics AS tb_film
LEFT JOIN title_ratings AS tb_notes ON tb_notes.tconst = tb_film.tconst
LEFT JOIN title_crew AS tb_crew ON tb_crew.tconst = tb_film.tconst
LEFT JOIN tb_regions ON tb_regions."titleId" = tb_film.tconst
LEFT JOIN tb_roles ON tb_film.tconst = tb_roles.tconst
;
'''
df = pd.read_sql(SQL, engine)
connection.close()

