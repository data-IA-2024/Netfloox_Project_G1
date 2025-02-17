-- Titles ratings table
-- cyril_netfloox.title_ratings definition

-- Drop table

-- DROP TABLE cyril_netfloox.title_ratings;

CREATE TABLE cyril_netfloox.title_ratings (
	tconst varchar(12) NULL,
	"averageRating" float4 NULL,
	"numVotes" int4 NULL
);

-- Titles basics table

-- DROP TABLE cyril_netfloox.title_basics;

CREATE TABLE cyril_netfloox.title_basics (
	tconst varchar(12) NULL,
	"titleType" varchar(250) NULL,
	"primaryTitle" text NULL,
	"originalTitle" text NULL,
	"isAdult" int4 NULL,
	"startYear" int4 NULL,
	"endYear" int4 NULL,
	"runtimeMinutes" int4 NULL,
	"genres" varchar(251) NULL
);

-- Titles crew table

--DROP TABLE cyril_netfloox.title_crew;

CREATE TABLE cyril_netfloox.title_crew (
	tconst varchar(12) NULL,
	"directors" text NULL,
	"writers" text NULL
);  

-- Titles episode table

-- DROP TABLE cyril_netfloox.title_episode;

CREATE TABLE cyril_netfloox.title_episode (
	tconst varchar(12) NULL,
	"parentTconst" varchar(250) NULL,
	"seasonNumber" int4 NULL,
	"episodeNumber" int4 NULL
);  

-- Titles principals table

--DROP TABLE cyril_netfloox.title_principals;

CREATE TABLE cyril_netfloox.title_principals (
	tconst varchar(12) NULL,
	"ordering" int4 NULL,
	"nconst" varchar(250) NULL,
	"category" TEXT NULL,
	"job" text NULL,
	"characters" text NULL
);

-- Names table

--DROP TABLE cyril_netfloox.name_basics;

CREATE TABLE cyril_netfloox.name_basics (
	nconst varchar(12) NULL,
	"primaryName" varchar(250) NULL,
	"birthYear" int4 NULL,
	"deathYear" int4 NULL,
	"primaryProfession" varchar(250) NULL,
	"knownForTitles" varchar(250) NULL
);

-- Titles akas table

--DROP TABLE cyril_netfloox.title_akas;

CREATE TABLE cyril_netfloox.title_akas (
	"titleId" varchar(12) NULL,
	"ordering" int4 NULL,
	"title" varchar(250) NULL,
	"region" varchar(251) NULL,
	"language" varchar(252) NULL,
	"types" varchar(253) NULL,
	"attributes" varchar(254) NULL,
	"isOriginalTitle" int4 NULL
);

-- Creation des Des clés primaires

--ALTER TABLE cyril_netfloox.title_akas ADD PRIMARY KEY (titleId);
--ALTER TABLE cyril_netfloox.title_basics ADD PRIMARY KEY (tconst);
--ALTER TABLE cyril_netfloox.title_crew ADD PRIMARY KEY (tconst);
--ALTER TABLE cyril_netfloox.title_episode ADD PRIMARY KEY (tconst);
--ALTER TABLE cyril_netfloox.title_principals ADD PRIMARY KEY (tconst);
--ALTER TABLE cyril_netfloox.name_basics ADD PRIMARY KEY (nconst);
--ALTER TABLE cyril_netfloox.title_ratings ADD PRIMARY KEY (tconst);

-- Requete de Jointure

--set search_path to echantillon_test;
--select "primaryTitle", "startYear", "endYear", "numVotes", "averageRating", "genres", "job", "characters"
--FROM title_basics tb
--left JOIN title_ratings tr ON tb.tconst=tr.tconst
--inner join title_principals tp on tb.tconst = tp.tconst
--left join name_basics nb on tp.nconst = nb.nconst
--left join title_crew tc on tc.tconst = tb.tconst;

-- 