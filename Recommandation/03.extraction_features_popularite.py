import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(r'variable_env/VAR.env')

nom_base_de_donnees = os.getenv("NOM_BASES_DONNEES")
utilisateur = os.getenv("USERAZURE")
mot_de_passe = os.getenv("PASSWORD")   
host = os.getenv("HOST")
port = os.getenv("PORT")
path_donnees = os.getenv("PATH_DONNEES")

def se_connecter_a_la_base_de_donnees():

    try:
        connexion = psycopg2.connect(
            dbname=nom_base_de_donnees,
            user=utilisateur,
            password=mot_de_passe,
            host=host,
            port=port
        )
        print("Connexion réussie à la base de données")
        return connexion
    except psycopg2.Error as e:
        print(f"Erreur lors de la connexion à la base de données: {e}")
        return None


connexion = se_connecter_a_la_base_de_donnees()

curseur = connexion.cursor()
# Charger les données de la table name_basics dans un DataFrame
#requete_name_basics = "SELECT * FROM echantillon_test.name_basics;"
#df_name_basics = pd.read_sql(requete_name_basics, connexion)
#print(df_name_basics.head())



#SQL= "SELECT *FROM cyril_netfloox.title_ratings"

SQL = '''SELECT 
tb_note."tconst" AS id_film
, tb_note."averageRating" AS note
, tb_note."titleType" AS type_film
, tb_note."startYear" AS annee_film
, split_part(genres, ',', 1) AS genre_1
, split_part(genres, ',', 2) AS genre_2
, split_part(genres, ',', 3) AS genre_3 
FROM cyril_netfloox."title_ratings" AS tb_note 
LEFT JOIN cyril_netfloox."title_basics" AS tb_basic ON tb_note."tconst" = tb_basic."tconst" 
ORDER BY tb_note."tconst" 
LIMIT 1000;'''

SQL = '''
SELECT MIN("averageRating") 
FROM cyril_netfloox."title_ratings";
'''

SQL = '''
SELECT *
FROM cyril_netfloox."name_basics"
WHERE "primaryName" LIKE '%Sean%' AND "deathYear" IS NOT NULL;
'''

SQL ='''
SELECT *
FROM cyril_netfloox."name_basics"
WHERE "primaryName" ILIKE '%john%' AND "primaryProfession" LIKE '%producer'
'''

SQL ='''
SELECT "titleType" AS type_film, COUNT(tconst) AS nb_id
FROM cyril_netfloox."title_basics"
GROUP BY "titleType"
'''

SQL ='''
SELECT "titleType" AS type_film, COUNT(tconst) AS nb_id
FROM cyril_netfloox."title_basics"
GROUP BY "titleType"
HAVING  COUNT(tconst) > 5000;
'''

SQL ='''
SELECT "titleType" AS genre, COUNT(tconst) as nb_id
FROM cyril_netfloox."title_basics"
WHERE "startYear" < 1950
GROUP BY "titleType"
HAVING  COUNT(tconst) > 5000;
'''

# JOINTURES

#on comence par extraire les features qui nous interesse pour la prediction de popularite
SQL ='''
SELECT tb_ratings.tconst AS id_film
, "averageRating" as target
, "numVotes"
, "titleType"
, "genres"
, split_part("genres", ',', 1) AS genre_1
, split_part("genres", ',', 2) AS genre_2
, split_part("genres", ',', 3) AS genre_3
, "isAdult"
, "startYear"
FROM cyril_netfloox."title_ratings" AS tb_ratings
LEFT JOIN cyril_netfloox."title_basics" AS tb_basics ON tb_ratings.tconst = tb_basics.tconst
LIMIT 1000;
'''

# En y ajoutant producers et actors
SQL ='''
WITH prep_ratings AS (

SELECT tb_ratings.tconst AS id_film
, "averageRating" as target
, "numVotes"
, "titleType"
, "genres"
, split_part("genres", ',', 1) AS genre_1
, split_part("genres", ',', 2) AS genre_2
, split_part("genres", ',', 3) AS genre_3
, "isAdult"
, "startYear"
-- Données appartenant à la table title_crew
, split_part("directors", ',', 1) AS id_director_1
, split_part("directors", ',', 2) AS id_director_2
, split_part("directors", ',', 3) AS id_director_3
, split_part("writers", ',', 3) AS id_writer_1
-- Données appartenant à la table name_basics

FROM cyril_netfloox."title_ratings" AS tb_ratings
LEFT JOIN cyril_netfloox."title_basics" AS tb_basics ON tb_ratings.tconst = tb_basics.tconst
LEFT JOIN cyril_netfloox."title_crew" AS tb_crew ON tb_ratings.tconst = tb_crew.tconst
-- On veut faire correspondre les id_directors à la tables des noms de ces personnes, on va utiliser les CTE 
-- en debut de requete avec WITH on encapsule notre code prededant dans une table temporaire.
--LEFT JOIN cyril_netfloox."name_basics" AS tb_name ON tb_crew.tconst = tb_name.nconst

)

SELECT prep_ratings.*
, tb_name_1."primaryName" AS director_1
, tb_name_2."primaryName" AS director_2
, tb_name_3."primaryName" AS director_3
, tb_name_4."primaryName" AS writer_1
FROM prep_ratings
LEFT JOIN cyril_netfloox."name_basics" AS tb_name_1 ON prep_ratings.id_director_1 = tb_name_1.nconst
LEFT JOIN cyril_netfloox."name_basics" AS tb_name_2 ON prep_ratings.id_director_2 = tb_name_2.nconst
LEFT JOIN cyril_netfloox."name_basics" AS tb_name_3 ON prep_ratings.id_director_3 = tb_name_3.nconst
LEFT JOIN cyril_netfloox."name_basics" AS tb_name_4 ON prep_ratings.id_writer_1 = tb_name_4.nconst;
'''


'''
SQL = "SELECT tb.primarytitle, tb.startyear, tr.numvotes, tr.averagerating 
FROM reference.title_basics tb 
INNER JOIN reference.title_ratings tr ON tb.tconst=tr.tconst 
LIMIT 10;"
'''

#SQL = '''SELECT tb_note."numVotes" FROM cyril_netfloox.title_ratings AS tb_note'''
df = pd.read_sql(SQL, connexion)
print(df)
curseur.close()
connexion.close()
