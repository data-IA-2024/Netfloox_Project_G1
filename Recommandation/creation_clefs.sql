-- Creation des clés primaires

ALTER TABLE cyril_netfloox.title_basics ADD PRIMARY KEY (tconst);
ALTER TABLE cyril_netfloox.title_crew ADD PRIMARY KEY (tconst);
ALTER TABLE cyril_netfloox.title_episode ADD PRIMARY KEY (tconst);
ALTER TABLE cyril_netfloox.name_basics ADD PRIMARY KEY (nconst);
ALTER TABLE cyril_netfloox.title_ratings ADD PRIMARY KEY (tconst);

-- Ci dessous on crée des clefs composées pour répondre à la contrainte d'unicité
ALTER TABLE cyril_netfloox.title_akas ADD PRIMARY KEY ("titleId", "ordering");
ALTER TABLE cyril_netfloox.title_principals ADD PRIMARY KEY (tconst, "ordering");

-- puis on ajoute la clef étrangère nconst de title_principals référencé comme cle primaire dans name_basics
-- ALTER TABLE cyril_netfloox.title_principals ADD CONSTRAINT "nconst_to_name_basics" FOREIGN KEY ("nconst") REFERENCES cyril_netfloox.name_basics("nconst");
