USE WAREHOUSE COMPUTE_WH;
CREATE DATABASE src;
CREATE DATABASE stg;
CREATE DATABASE dwh;

---
--- Source database
---

USE DATABASE src;
CREATE SCHEMA source;
USE SCHEMA source;

CREATE OR REPLACE TABLE src_produit (
    "Id_Produit" NUMBER(3,0),
    "Lib_Produit" VARCHAR(8),
    "Desc_Produit" VARCHAR(20),
    CONSTRAINT SRC_PRODUIT_PK PRIMARY KEY ("Id_Produit")
);


INSERT INTO
    src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (1, 'Voix', 'Appel vocal');

INSERT INTO
    src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (2, 'SMS', 'Message SMS');

INSERT INTO
    src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (3, 'Fax', 'Envoi vers fax');

INSERT INTO
    src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (0, 'Unknown', '???');
    
USE DATABASE SRC;
USE SCHEMA source;
select * from src_produit;
truncate table src_produit;

---
--- Staging database
---
USE DATABASE stg;
CREATE SCHEMA staging;
USE SCHEMA staging;

CREATE OR REPLACE TABLE STG.STAGING.STG_OFFRE (
	"Id_Offre" NUMBER(3,0),
	"Lib_Offre" VARCHAR(15),
	"Desc_Offre" VARCHAR(30),
    CONSTRAINT STG_OFFRE_PK PRIMARY KEY ("Id_Offre")
);

CREATE OR REPLACE TABLE STG.STAGING.STG_DISTANCE (
	"Id_Distance" NUMBER(2,0),
	"Lib_Distance" VARCHAR(50),
	"Desc_Distance" VARCHAR(50),
    CONSTRAINT STG_DISTANCE_PK PRIMARY KEY ("Id_Distance")
);

CREATE OR REPLACE TABLE STG.STAGING.STG_DIRECTION (
    "Id_Direction" NUMBER(2,0),
    "Lib_Direction" VARCHAR(15),
    CONSTRAINT STG_CLIENT_PK PRIMARY KEY ("Id_Direction")
);


CREATE OR REPLACE TABLE STG.STAGING.STG_CLIENT (
    "Id_Client" NUMERIC(10,0),
    "Nom_Client" VARCHAR(15),
    "Prenom_Client" VARCHAR(20),
    "Numero" VARCHAR(10),
    "Id_Offre" NUMERIC(3,0),
    "Date_abonnement" DATE,
     CONSTRAINT STG_DIRECTION_PK PRIMARY KEY ("Id_Client")
);

CREATE OR REPLACE TABLE STG.STAGING.STG_PRODUIT (
    "Id_Produit" NUMBER(3,0),
    "Lib_Produit" VARCHAR(8),
    "Desc_Produit" VARCHAR(20),
    CONSTRAINT STG_PRODUIT_PK PRIMARY KEY ("Id_Produit")
);

CREATE OR REPLACE TABLE STG.STAGING.STG_APPEL (
    "Id_Client" NUMBER(10,0),
    "Date_appel" DATE,
    "Heure_appel" VARCHAR(5),
    "No_appelant" VARCHAR(20),
    "No_appele" VARCHAR(20),
    "Id_Direction" NUMBER(1,0),
    "Id_Produit" NUMBER(1,0),
    "Id_Distance" NUMBER(2,0),
    "Duree" NUMBER(5,0),
    CONSTRAINT STG_APPEL_PK PRIMARY KEY ("Date_appel", "Heure_appel")
);

USE DATABASE stg;
USE SCHEMA staging;

SELECT * FROM stg_offre;
SELECT * FROM stg_distance;
SELECT * FROM stg_direction;
SELECT * FROM stg_client;
SELECT * FROM stg_produit;
SELECT * FROM stg_appel;

TRUNCATE TABLE stg_offre;
TRUNCATE TABLE stg_distance;
TRUNCATE TABLE stg_direction;
TRUNCATE TABLE stg_client;
TRUNCATE TABLE stg_produit;
TRUNCATE TABLE stg_appel;

---
--- Normalized database
---

USE DATABASE DWH;
CREATE SCHEMA normalized;
USE SCHEMA normalized;

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_DIRECTION (
    "Id_Direction" NUMBER(2,0),
    "Lib_Direction" VARCHAR(15),
    CONSTRAINT DWH_CLIENT_PK PRIMARY KEY ("Id_Direction")
);

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_PRODUIT (
    "Id_Produit" NUMBER(3,0),
    "Lib_Produit" VARCHAR(8),
    "Desc_Produit" VARCHAR(20),
    CONSTRAINT DWH_PRODUIT_PK PRIMARY KEY ("Id_Produit")
);

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_DISTANCE (
	"Id_Distance" NUMBER(2,0),
	"Lib_Distance" VARCHAR(50),
	"Desc_Distance" VARCHAR(50),
    "Reseau" VARCHAR(7),
    CONSTRAINT DWH_DISTANCE_PK PRIMARY KEY ("Id_Distance")
);

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_OFFRE (
	"Id_Offre" NUMBER(3,0),
	"Lib_Offre" VARCHAR(15),
	"Desc_Offre" VARCHAR(30),
    "Type_Offre" VARCHAR(8),
    CONSTRAINT DWH_OFFRE_PK PRIMARY KEY ("Id_Offre")
);

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_CLIENT (
    "Id_Client" NUMBER(10,0),
    "Nom_Client" VARCHAR(15),
    "Prenom_Client" VARCHAR(20),
    "Numero" VARCHAR(10),
    "Id_Offre" NUMBER(3,0),
    "Lib_Offre" VARCHAR(15),
    "Type_Offre" VARCHAR(8),
    "Date_abonnement" DATE,
    "Trim_abonnement" VARCHAR(6),
     CONSTRAINT DWH_DIRECTION_PK PRIMARY KEY ("Id_Client")
);


CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_APPEL (
    "Id_Client"	NUMBER(10,0),
    "Date_appel"	DATE,
    "Heure_appel"	VARCHAR(5),
    "Lib_Offre"	VARCHAR(15),
    "Type_Offre"	VARCHAR(8),
    "No_appelant"	VARCHAR(20),
    "No_appele"	VARCHAR(20),
    "Id_Direction"	NUMBER(1,0),
    "Id_Produit"	NUMBER(1,0),
    "Lib_Produit"	VARCHAR(8),
    "Id_Distance"	NUMBER(2,0),
    "Lib_Distance"	VARCHAR(15),
    "Reseau"		VARCHAR(10),
    "Duree"		NUMBER(5,0),
    "Pays"		VARCHAR(15),
    Constraint DWH_APPEL_PK Primary key ("Id_Client", "Date_appel", "Heure_appel")
);

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_AGG_APPEL_PRD (
    "Lib_Produit"	VARCHAR(8),
    "Mois_appel"	VARCHAR(6),
    "Type_Offre"	VARCHAR(8),
    "Id_Direction"	NUMBER(1,0),
    "Lib_Distance"	VARCHAR(15),
    "Reseau"		VARCHAR(10),
    "Duree"		NUMBER(5,0),
    "Nb_appel"	NUMBER(5,0),
    Constraint DWH_AGG_APPEL_PRD_PK Primary key ("Lib_Produit", "Mois_appel", "Type_Offre", "Id_Direction", "Lib_Distance", "Reseau")
);

CREATE OR REPLACE TABLE DWH.NORMALIZED.DWH_AGG_APPEL_DISTANCE (
    "Lib_Distance"	VARCHAR(15),
    "Pays"		VARCHAR(15),
    "Mois_appel"	VARCHAR(6),
    "Type_Offre"	VARCHAR(8),
    "Id_Direction"	NUMBER(1,0),
    "Lib_Produit"	VARCHAR(8),
    "Duree"		NUMBER(5),
    "Nb_appel"	NUMBER(5),
    Constraint DWH_AGG_APPEL_DISTANCE_PK Primary key ("Lib_Distance", "Pays", "Mois_appel", "Type_Offre", "Id_Direction", "Lib_Produit")
);

USE DATABASE dwh;
USE SCHEMA normalized;

SELECT * FROM DWH_DIRECTION;
SELECT * FROM DWH_DISTANCE;

TRUNCATE TABLE DWH_DIRECTION;
TRUNCATE TABLE DWH_DISTANCE;