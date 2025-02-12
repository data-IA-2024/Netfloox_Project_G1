import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import os
from dotenv import load_dotenv

#from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
#from sklearn.preprocessing import Normalizer, StandardScaler
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer
#from sklearn.pipeline import Pipeline
from sklearn.pipeline import make_pipeline
import fct_extract as my_fct
from sqlalchemy import create_engine
import joblib
import json
import requests
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

def se_connecter_a_la_base_de_donnees():
    nom_base_de_donnees = 'postgres'
    utilisateur = 'psqladmin'
    mot_de_passe = 'GRETAP4!2025***'   
    host = 'netfloox-psqlflexibleserver-1.postgres.database.azure.com'
    port = '5432'

    try:
        connexion = psycopg2.connect(
            dbname=nom_base_de_donnees,
            user=utilisateur,
            password=mot_de_passe,
            host=host,
            port=port
        )
        return connexion
    except psycopg2.Error as e:
        st.write(f"Erreur lors de la connexion à la base de données: {e}")
        return None
    
# Initialiser la variable page dans le session state
if 'page' not in st.session_state:
    st.session_state.page = "Accueil"
    
def extraire_donnees_BDD():
    load_dotenv(r'Projet_NexFloox\variable_env\VAR.env')
    nom_base_de_donnees = 'postgres'
    utilisateur = 'psqladmin'
    mot_de_passe = 'GRETAP4!2025***'   
    host = 'netfloox-psqlflexibleserver-1.postgres.database.azure.com'
    port = '5432'

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

    return df

# Afficher la selectbox pour la navigation entre les pages
st.session_state.page = st.selectbox("Choisissez une page :", ["Accueil", "Recommandations", "À propos"], index=["Accueil", "Recommandations", "À propos"].index(st.session_state.page))

# Utiliser la page du session state
page = st.session_state.page



# page accueil
if page == "Accueil":
    connexion = se_connecter_a_la_base_de_donnees()
    st.markdown("<h1 style='text-align: center;'>Netfloox</h1>", unsafe_allow_html=True)
    st.markdown("<h2 style='text-align: center;'>Application de recommandation de films et de séries</h2>", unsafe_allow_html=True)
    # Récupérer le top 5 des acteurs avec 2 ou 3 films respectifs
    def obtenir_top_acteurs_et_films(connexion):
        try:
            curseur = connexion.cursor()
                
            requete = """
            SELECT a."primaryName", array_agg(t."primaryTitle") as films
            FROM echantillon_test.title_principals tp
            JOIN echantillon_test.name_basics a ON tp.nconst = a.nconst
            JOIN echantillon_test.title_basics t ON tp.tconst = t.tconst
            WHERE tp.category = 'actor' OR tp.category = 'actress'
            GROUP BY a."primaryName"
            HAVING COUNT(t."primaryTitle") BETWEEN 2 AND 3
            ORDER BY COUNT(t."primaryTitle") DESC
            LIMIT 5;
            """
            curseur.execute(requete)
            resultats = curseur.fetchall()
            return resultats
        except psycopg2.Error as e:
            st.error(f"Erreur lors de la récupération des données: {e}")
            return []

    if connexion:
        top_acteurs_et_films = obtenir_top_acteurs_et_films(connexion)

        st.markdown("<h2 style='text-align: center;'>Top 5 des acteurs</h2>", unsafe_allow_html=True)
        for acteur, films in top_acteurs_et_films:
            st.markdown(f"<h3 style='text-align: left;'><em>{acteur}</em></h3>", unsafe_allow_html=True)
            st.write("**Films :**")
            for film in films:
                st.write(f"- {film}")
        df = pd.DataFrame(top_acteurs_et_films, columns=["Acteur", "Films"], index=range(1, 6))
        st.table(df)
#----------------------------------------------------------------------------------
    # Top 5 des genres avec 2 ou 3 films respectifs
    def obtenir_top_genres_et_films(connexion):
        try:
            curseur = connexion.cursor()
            requete = """
                WITH genre_avg AS (
                    SELECT split_part(t."genres", ',', 1) AS genre, 
                        AVG(r."averageRating") as avg_rating
                    FROM echantillon_test.title_basics t
                    JOIN echantillon_test.title_ratings r ON t.tconst = r.tconst
                    WHERE t."genres" IS NOT NULL
                        AND split_part(t."genres", ',', 1) NOT IN ('Music', 'Documentary', 'Short', 'Reality-TV', 'Talk-Show', 'Game-Show')
                    GROUP BY genre
                ),
                ranked_films AS (
                    SELECT split_part(t."genres", ',', 1) AS genre, 
                        t."primaryTitle" as film,
                        r."averageRating",
                        ROW_NUMBER() OVER (PARTITION BY split_part(t."genres", ',', 1) ORDER BY r."averageRating" DESC) as rn
                    FROM echantillon_test.title_basics t
                    JOIN echantillon_test.title_ratings r ON t.tconst = r.tconst
                    WHERE t."genres" IS NOT NULL
                        AND split_part(t."genres", ',', 1) NOT IN ('Music', 'Documentary', 'Short', 'Reality-TV', 'Talk-Show', 'Game-Show')
                )
                SELECT rf.genre, 
                    array_agg(rf.film) as films,
                    ga.avg_rating
                FROM ranked_films rf
                JOIN genre_avg ga ON rf.genre = ga.genre
                WHERE rf.rn <= 5
                GROUP BY rf.genre, ga.avg_rating
                ORDER BY ga.avg_rating DESC
                LIMIT 5;
            """
            curseur.execute(requete)
            resultats = curseur.fetchall()
            return resultats
        except psycopg2.Error as e:
            st.write(f"Erreur lors de la récupération des données: {e}")
            return []
    
    if connexion:
        top_genres_et_films = obtenir_top_genres_et_films(connexion)

        st.subheader("Top 5 des genres avec 2 ou 3 films respectifs")
        for genre, films, avg_rating in top_genres_et_films:
            st.markdown(f"<h3 style='text-align: left;'><em>{genre}</em></h3>", unsafe_allow_html=True)
            st.write("**Films :**")
            for film in films:
                st.write(f"- {film}")
            st.write(f"Note moyenne: {avg_rating}")
        
        df = pd.DataFrame(top_genres_et_films, columns=["Genre", "Films", "Note moyenne"], index=range(1, 6))
        st.table(df)

    def obtenir_top_producters_et_films(connexion):
        try:
            curseur = connexion.cursor()
            requete = """
                SELECT a."primaryName" AS producer, 
                array_agg(t."primaryTitle") as films,
                AVG(r."averageRating") as avg_rating
                FROM echantillon_test.title_principals tp
                JOIN echantillon_test.name_basics a ON tp.nconst = a.nconst
                JOIN echantillon_test.title_basics t ON tp.tconst = t.tconst
                JOIN echantillon_test.title_ratings r ON t.tconst = r.tconst
                WHERE tp.category = 'producer'
                GROUP BY a."primaryName"
                HAVING Count (t."primaryTitle") > 3 and AVG(r."averageRating") > 5.0
                ORDER BY avg_rating DESC
                LIMIT 5;
            """
            curseur.execute(requete)
            resultats = curseur.fetchall()
            return resultats
        except psycopg2.Error as e:
            st.write(f"Erreur lors de la récupération des données: {e}")
            return []
    
    if connexion:
        top_producters_et_films = obtenir_top_producters_et_films(connexion)

        st.subheader("Top 5 des producteurs avec 2 ou 3 films respectifs")
        for producter, films, avg_rating in top_producters_et_films:
            st.markdown(f"<h3 style='text-align: left;'>Producteur: <em>{producter}</em></h3>", unsafe_allow_html=True)
            st.write("**Films :**")
            for i, film in enumerate(films):
                if i >= 5:  # Limite à 5 films
                    break
                st.write(f"- {film}")
            st.write(f"Note moyenne: {avg_rating}")
        
        df = pd.DataFrame(top_producters_et_films, columns=["Producteur", "Films", "Note moyenne"], index=range(1, 6))
        st.table(df)
#----------------------------------------------------------------------------------
    st.subheader("Analyse statistique des tables")
    def observer_taux_remplissage_par_table(connexion):
        try:
            curseur = connexion.cursor()
            requete_tables = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'echantillon_test';"
            curseur.execute(requete_tables)
            tables = curseur.fetchall()
        
            data = []
        
            for table in tables:
                nom_table = table[0]
                requete_count = f"SELECT COUNT(*) FROM echantillon_test.{nom_table};"
                curseur.execute(requete_count)
                nombre_lignes = curseur.fetchone()[0]
            
                requete_taille = f"SELECT pg_total_relation_size('echantillon_test.{nom_table}');"
                curseur.execute(requete_taille)
                taille_table = curseur.fetchone()[0]
            
                requete_taille_remplie = f"SELECT pg_relation_size('echantillon_test.{nom_table}');"
                curseur.execute(requete_taille_remplie)
                taille_remplie_table = curseur.fetchone()[0]
            
                taux_remplissage = (taille_remplie_table / taille_table * 100) if taille_table > 0 else 0
                data.append([nom_table, nombre_lignes, taille_table, taille_remplie_table, taux_remplissage])
        
            df = pd.DataFrame(data, columns=["Table", "Nombre de lignes", "Taille totale", "Taille remplie", "Taux de remplissage"])
            st.table(df[["Table", "Nombre de lignes", "Taux de remplissage"]])
            # Afficher le taux de remplissage par table par graphique
            fig, ax = plt.subplots()
            df.plot(kind='bar', x='Table', y='Taux de remplissage', ax=ax, legend=False)
            ax.set_ylabel('Taux de remplissage')
            ax.set_title('Taux de remplissage par table')
            st.pyplot(fig)
        
            curseur.close()
        except psycopg2.Error as e:
            st.write(f"Erreur lors de l'observation du taux de remplissage par table: {e}")
    if connexion:
        observer_taux_remplissage_par_table(connexion)

elif page == "Recommandations":
    st.markdown("<h4 style='text-align: center;'>Vous ne savez pas quoi regarder ? Netfloox vous propose des recommandations en fonction de vos goûts !</h4>", unsafe_allow_html=True)

    film_saisie = st.text_input("Saisissez un film: ")
    #Fonction pour recommander des films en fonction d'un film de référence
    def recommander_films():
        df = extraire_donnees_BDD()

        #data = df[['tconst', 'primaryTitle', 'genres', 'averageRating', 'numVotes', 'startYear', 'isAdult', 'titleType']]
        data = df[['primaryTitle', 'genres', 'averageRating', 'isAdult', 'titleType']]

        # Créer une nouvelle colonne 'features' qui combine les informations textuelles
        #col_features = ['director', 'producer', 'actor', 'actress', 'primaryTitle', 'genres']
        col_features = ['director', 'producer', 'actor', 'actress', 'genres']
        data.loc[:, 'features'] = ""
        for col in col_features:
            data.loc[:, col] = df[col]
            data.loc[:, col] = data[col].fillna("")
            data.loc[:, 'features'] = data['features'] + "," + data[col].apply(lambda x: ' '.join(x if isinstance(x, list) else [str(x)])).fillna('')
            # Je rajoute l'effacement de la colonne traitée
            data.drop(columns=col, inplace=True)
        data.loc[:, 'features'] = data['features'].apply(lambda x: x.replace(",", " ",))
        data.loc[:, 'features'] = data['features'].str.strip()

        def get_index_from_title(df, title):
            # simple exemple a adapter
            #print(df[df['primaryTitle'].fillna('').str.contains(title)].index)
            return df[df['primaryTitle'].fillna('').str.contains(title)]

        vectorizer = CountVectorizer()
        svd = TruncatedSVD(n_components=10)  # Ajustez le nombre de composants selon vos besoins
        pipeline = make_pipeline(vectorizer, svd)

        # Transformation des caractéristiques textuelles
        features_transformed = pipeline.fit_transform(data['features'])
        # On passe de float64 a float16
        features_transformed = features_transformed.astype(np.float16)

        # On sauvegarde la matrice de transformation
        file = 'features_transformed.npy'
        np.save(file, features_transformed)

        # On choisit un titre
        title = 'Enchanted Cup'
        movie_idx = get_index_from_title(df, film_saisie).index[0]  # Index[0] permet de choisir le premier film qui sort

        X = np.load(file)
        Y = features_transformed[movie_idx, :]

        # calcule des similarités
        cosine_sim = cosine_similarity(X, Y.reshape(1, -1))

        # On récupère le n films les plus semblables
        # Pas encore tout compris mais ca marche ! ;-) 
        n = 6
        cosine_sim_1d = cosine_sim.flatten()
        idxs = np.argpartition(cosine_sim_1d, -n)[-n:]
        idxs = idxs[np.argsort(cosine_sim_1d[idxs])][::-1]

        # affichage des recommandations
        st.write('Recommandations : ')
        st.write(df.loc[idxs, 'primaryTitle'])
        movie_data = requests.get(f"https://www.omdbapi.com/?apikey=49377d70&t={film_saisie}").text
        movie_json = json.loads(movie_data)
        st.image(movie_json.get("Poster", ""), width=150)

    def popularite_film(film_saisie):
        transfo_text = joblib.load("transfo_text_primaryTitle.joblib")

        # Transformer le titre du film saisi
        film_feature = transfo_text.transform([film_saisie])

        # Charger le modèle préalablement entraîné
        model = joblib.load("reglog_1.joblib")

        # Prédire la popularité du film
        resultat = model.predict(film_feature)
        st.write(resultat)
    #Si le film saisie est vide, on affiche un message
    if film_saisie =="":
        st.write("Vous n'avez pas saisie de film")
    else: #Si le film saisie n'est pas vide, on recommande des films
        recommander_films()
        popularite_film(film_saisie)
elif page == "À propos":
    st.write("À propos de Netfloox.")

button_close_connexion = st.button("Fermer la connexion")

if button_close_connexion:
    connexion.close()
    st.stop()




