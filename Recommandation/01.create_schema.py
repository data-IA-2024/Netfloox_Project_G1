import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv


load_dotenv(r'variable_env/VAR.env')

path_donnees = os.getenv("PATH_DONNEES")

def se_connecter_a_la_base_de_donnees():
    nom_base_de_donnees = os.getenv("NOM_BASES_DONNEES")
    utilisateur = os.getenv("USERAZURE")
    mot_de_passe = os.getenv("PASSWORD")   
    host = os.getenv("HOST")
    port = os.getenv("PORT")

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


def create_schema(connexion, schema_name):
    curseur = connexion.cursor()
    curseur.execute(f"DROP SCHEMA {schema_name} CASCADE") # Efface le schema et tous les objets qu'il contient !!!
    curseur.execute(f"CREATE SCHEMA {schema_name}")
    connexion.commit()
    curseur.close()


def import_sql_requests_from_file(connexion, file):
    try:
        # Création du curseur
        curseur = connexion.cursor()
        # Lecture 
        with open(file, 'r', encoding='utf-8') as sql_file:
            requetes_sql = sql_file.read()
        # Exécution des requetes sql provenant du fichier
        curseur.execute(requetes_sql)
        # Validation des changements
        connexion.commit()
        print(f"Le fichier {file} a été importé avec succès")

        curseur.close()

    except Exception as e:
        print(f"Erreur {e} lors de l'importation du fichier {file}")

connexion = se_connecter_a_la_base_de_donnees()
nom_schema = "cyril_netfloox"
create_schema(connexion, nom_schema)

# Creation des tables 
path_fichier_sql = "./creation_tables.sql" # Attention le fichier fait référence au schema cyril_netfloox
import_sql_requests_from_file(connexion, path_fichier_sql)

# Creation des clefs primires
# On pourrait tester l'unicité des clefs avec pandas et le code suivant : 
## df = pd.read_csv("file.csv")
## doublons = df[df.duplicated(subset=["MyPKey"], keep=False]]
## if doublons ....


import_sql_requests_from_file(connexion, "./creation_clefs.sql")

## On peut voir ces clefs dans DBeaver->Indexes



connexion.close()


