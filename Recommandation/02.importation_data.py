import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv(r'variable_env/VAR.env')

path_donnees = os.getenv("PATH_DONNEES")
nom_base_de_donnees = os.getenv("NOM_BASES_DONNEES")
utilisateur = os.getenv("USERAZURE")
mot_de_passe = os.getenv("PASSWORD")   
host = os.getenv("HOST")
port = os.getenv("PORT")

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

def importer_donnees(connexion):
    if connexion:
        n_rows = 200000
        
        database_url = f"postgresql://{utilisateur}:{mot_de_passe}@{host}:{port}/{nom_base_de_donnees}"
        engine = create_engine(database_url)
        
        fichiers_tsv = ['title.basics.tsv.gz', 'title.principals.tsv.gz', 'title.akas.tsv.gz', 'title.crew.tsv.gz', 'title.episode.tsv.gz', 'title.ratings.tsv.gz', 'name.basics.tsv.gz'] 
        for fichier in fichiers_tsv:
            chemin_fichier = os.path.join(path_donnees, fichier)
            
            nom_table = fichier.split('.')[0] + '_' + fichier.split('.')[1]
            curseur = connexion.cursor()
            requete_tables = f"TRUNCATE TABLE cyril_netfloox.{nom_table};"
            curseur.execute(requete_tables)
            connexion.commit()
            df = pd.read_csv(chemin_fichier, sep='\t', compression='gzip', low_memory=False, nrows=n_rows,na_values=['\\N'] )#

            try:
                df.to_sql(nom_table, engine, schema='cyril_netfloox', if_exists='append', index=False)
                print(f"Table {nom_table} remplie avec succès.")
            except Exception as e:
                print(f"Erreur lors de la création de la table {nom_table}: {e}")
        

def observer_nombre_lignes_et_taux_remplissage(connexion):
    try:
        curseur = connexion.cursor()
        requete_tables = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'cyril_netfloox';"
        curseur.execute(requete_tables)
        tables = curseur.fetchall()
        
        total_taille = 0
        total_taille_remplie = 0
        
        for table in tables:
            nom_table = table[0]
            requete_count = f"SELECT COUNT(*) FROM cyril_netfloox.{nom_table};"
            curseur.execute(requete_count)
            nombre_lignes = curseur.fetchone()[0]
            print(f"Table {nom_table} a {nombre_lignes} lignes.")
            
            requete_taille = f"SELECT pg_total_relation_size('cyril_netfloox.{nom_table}');"
            curseur.execute(requete_taille)
            taille_table = curseur.fetchone()[0]
            total_taille += taille_table
            
            requete_taille_remplie = f"SELECT pg_relation_size('cyril_netfloox.{nom_table}');"
            curseur.execute(requete_taille_remplie)
            taille_remplie_table = curseur.fetchone()[0]
            total_taille_remplie += taille_remplie_table
        
        if total_taille > 0:
            taux_remplissage_global = total_taille_remplie / total_taille
            print(f"Taux de remplissage global des tables est {taux_remplissage_global:.2%}.")
        else:
            print("Impossible de calculer le taux de remplissage global car la taille totale est zéro.")
        
        curseur.close()
        connexion.close()
    except psycopg2.Error as e:
        print(f"Erreur lors de l'observation du nombre de lignes et du taux de remplissage: {e}")
        connexion.close()
        


connexion = se_connecter_a_la_base_de_donnees()
importer_donnees(connexion)
observer_nombre_lignes_et_taux_remplissage(connexion)

