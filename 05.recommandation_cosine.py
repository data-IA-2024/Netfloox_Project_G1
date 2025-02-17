import pandas as pd
import numpy as np
#from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
#from sklearn.preprocessing import Normalizer, StandardScaler
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer
#from sklearn.pipeline import Pipeline
from sklearn.pipeline import make_pipeline
import fct_extract as my_fct
import gc


# On choisit un titre
title = 'Enchanted Cup'

df = my_fct.extraire_donnees_BDD()

#data = df[['tconst', 'primaryTitle', 'genres', 'averageRating', 'numVotes', 'startYear', 'isAdult', 'titleType']]
data = df[['primaryTitle', 'genres', 'averageRating', 'isAdult', 'titleType']]

# Créer une nouvelle colonne 'features' qui combine les informations textuelles
#col_features = ['director', 'producer', 'actor', 'actress', 'primaryTitle', 'genres']
col_features = ['director', 'producer', 'actor', 'actress', 'genres']
data.loc[:,'features'] = ""
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


#On récupère l'index du film dont on recherche le titre
movie_idx = get_index_from_title(df, title).index[0] # Index[0] permet de choisir le premier film qui sort

# On charge la matrice de transformation puis on recupére le vecteur du film voulu
X = np.load(file)
Y = features_transformed[movie_idx, :]

# calcule des similarités
cosine_sim = cosine_similarity(X, Y.reshape(1,-1))

# On récupère le n films les plus semblables
# Pas encore tout compris mais ca marche ! ;-) 
n=5
cosine_sim_1d = cosine_sim.flatten()
idxs = np.argpartition(cosine_sim_1d, -n)[-n:]
idxs = idxs[np.argsort(cosine_sim_1d[idxs])][::-1]

# affichage des recommandations
print('Recommandations : ')
print(df.loc[idxs])