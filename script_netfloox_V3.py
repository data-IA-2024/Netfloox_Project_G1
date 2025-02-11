from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import gzip
import requests

load_dotenv(r"Projet_NexFloox\variable_env\VAR.env")

# Récupération des variables d'environnement
nom_base_de_donnees = os.getenv("NOM_BASES_DONNEES")
utilisateur = os.getenv("USER")
mot_de_passe = os.getenv("PASSWORD")   
host = os.getenv("HOST")
port = os.getenv("PORT")
path_donnees = os.getenv("PATH_DONNEES")

if not all([nom_base_de_donnees, utilisateur, mot_de_passe, host, port]):
    print("Erreur : Une ou plusieurs variables d'environnement ne sont pas définies.")
    exit(1)

# Création de l'URL de connexion à la base de données
database_url = f"postgresql://{utilisateur}:{mot_de_passe}@{host}:{port}/{nom_base_de_donnees}"
engine = create_engine(database_url) 



def se_connecter_a_la_base_de_donnees():
    """Connexion à la base de données PostgreSQL."""
    nom_base_de_donnees = os.getenv("NOM_BASES_DONNEES")
    utilisateur = os.getenv("USER")
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

def telecharger_fichier(url, chemin_destination):
    """Télécharge un fichier depuis une URL."""
    try:
        os.makedirs(os.path.dirname(chemin_destination), exist_ok=True)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(chemin_destination, 'wb') as fichier:
            for chunk in response.iter_content(chunk_size=8192):
                fichier.write(chunk)
        print(f"Fichier téléchargé avec succès : {chemin_destination}")
    except requests.RequestException as e:
        print(f"Erreur lors du téléchargement du fichier : {e}")

def inserer_donnees_en_bulk(connexion, table, donnees):
    """Insère les données en bulk pour une importation plus rapide."""
    try:
        with connexion.cursor() as curseur:
            colonnes = list(donnees[0].keys())
            colonnes_formattees = ", ".join([f'"{col}"' for col in colonnes])
            requete = f"INSERT INTO {table} ({colonnes_formattees}) VALUES %s"
            valeurs = [[d[col] for col in colonnes] for d in donnees]
            psycopg2.extras.execute_values(curseur, requete, valeurs, page_size=10000)
            connexion.commit()
            print(f"Insertion bulk terminée avec succès pour {len(valeurs)} lignes.")
    except psycopg2.Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")
        connexion.rollback()

def traiter_et_inserer_fichier_par_lots(chemin_fichier, connexion, table, taille_lot):
    """Lit et insère un fichier TSV en streaming pour éviter les problèmes de mémoire."""
    try:
        with gzip.open(chemin_fichier, 'rt', encoding='utf-8') as f:
            reader = pd.read_csv(f, sep='\t', chunksize=taille_lot, low_memory=False, na_values=['\\N'], on_bad_lines='skip')
            for i, chunk in enumerate(reader):
                donnees = chunk.to_dict(orient='records')
                inserer_donnees_en_bulk(connexion, table, donnees)
                print(f"Chunk {i + 1} traité avec succès.")
    except Exception as e:
        print(f"Erreur lors du traitement du fichier : {e}")

if __name__ == "__main__":
    load_dotenv()
    connexion = se_connecter_a_la_base_de_donnees()

    if connexion:
        chemin_fichier = os.path.join(path_donnees, "title.basics.tsv.gz")
        traiter_et_inserer_fichier_par_lots(chemin_fichier, connexion, "netfloox.title_basics", taille_lot=200000)
        connexion.close()
