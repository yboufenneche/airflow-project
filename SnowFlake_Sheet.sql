USE WAREHOUSE COMPUTE_WH;

---
--- User Defined Functions
---

--- Function pays(): extract the country based on distanc ID, calling number, and called number

CREATE OR REPLACE FUNCTION pays(id_disantce int, no_appelant string, no_appele string)
returns string
language python
runtime_version = '3.10'
handler = 'pays_py'
AS
$$
def pays_py(id_distance, no_appelant, no_appele):
    if (id_distance != 5):
        return 'France'
    if (no_appelant.startswith("30") or no_appele.startswith("30")):
        return "Grèce"
    if (no_appelant.startswith("33") or no_appele.startswith("33")):
        return "Fracne"
    if (no_appelant.startswith("352") or no_appele.startswith("352")):
        return "Luxembourg"
    if (no_appelant.startswith("377") or no_appele.startswith("377")):
        return "Monaco"
    if (no_appelant.startswith("41") or no_appele.startswith("41")):
        return "Suisse"
    if (no_appelant.startswith("44") or no_appele.startswith("44")):
        return "Royaume Uni"
    if (no_appelant.startswith("45") or no_appele.startswith("45")):
        return "Danemark"
    if (no_appelant.startswith("49") or no_appele.startswith("49")):
        return "Allemagne"

    return "Autre"
$$; 

--- Create databases

CREATE DATABASE src;
CREATE DATABASE stg;
CREATE DATABASE dwh;

---
--- DDL source database
---

USE DATABASE src;
CREATE SCHEMA source;
USE SCHEMA source;

CREATE OR REPLACE TABLE SRC.SOURCE.src_produit (
    "Id_Produit" NUMBER(3,0),
    "Lib_Produit" VARCHAR(8),
    "Desc_Produit" VARCHAR(20),
    CONSTRAINT SRC_PRODUIT_PK PRIMARY KEY ("Id_Produit")
);

---
--- DDL staging database
---

USE DATABASE stg;
CREATE SCHEMA staging;

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

---
--- DDL normalized database
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

---
--- Insert data to SRC database
---

INSERT INTO
    SRC.SOURCE.src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (1, 'Voix', 'Appel vocal');

INSERT INTO
    SRC.SOURCE.src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (2, 'SMS', 'Message SMS');

INSERT INTO
    SRC.SOURCE.src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (3, 'Fax', 'Envoi vers fax');

INSERT INTO
    SRC.SOURCE.src_produit ("Id_Produit", "Lib_Produit", "Desc_Produit")
VALUES
    (0, 'Unknown', '???');

---
--- Select data
----

SELECT * FROM SRC.SOURCE.src_produit

SELECT * FROM STG.STAGING.stg_offre;
SELECT * FROM STG.STAGING.stg_distance;
SELECT * FROM STG.STAGING.stg_direction;
SELECT * FROM STG.STAGING.stg_client;
SELECT * FROM STG.STAGING.stg_produit;
SELECT * FROM STG.STAGING.stg_appel;

SELECT * FROM DWH.NORMALIZED.DWH_DIRECTION;
SELECT * FROM DWH.NORMALIZED.DWH_DISTANCE;
SELECT * FROM DWH.NORMALIZED.DWH_PRODUIT;
SELECT * FROM DWH.NORMALIZED.DWH_OFFRE;
SELECT * FROM DWH.NORMALIZED.DWH_CLIENT;
SELECT * FROM DWH.NORMALIZED.DWH_APPEL;
SELECT * FROM DWH.NORMALIZED.DWH_AGG_APPEL_PRD;
SELECT * FROM DWH.NORMALIZED.DWH_AGG_APPEL_DISTANCE;

---
--- Truncate data 
---

TRUNCATE TABLE STG.STAGING.stg_offre;
TRUNCATE TABLE STG.STAGING.stg_distance;
TRUNCATE TABLE STG.STAGING.stg_direction;
TRUNCATE TABLE STG.STAGING.stg_client;
TRUNCATE TABLE STG.STAGING.stg_produit;
TRUNCATE TABLE STG.STAGING.stg_appel;

TRUNCATE TABLE DWH.NORMALIZED.DWH_DIRECTION;
TRUNCATE TABLE DWH.NORMALIZED.DWH_DISTANCE;
TRUNCATE TABLE DWH.NORMALIZED.DWH_PRODUIT;
TRUNCATE TABLE DWH.NORMALIZED.DWH_OFFRE;
TRUNCATE TABLE DWH.NORMALIZED.DWH_CLIENT;
TRUNCATE TABLE DWH.NORMALIZED.DWH_APPEL;
TRUNCATE TABLE DWH.NORMALIZED.DWH_AGG_APPEL_PRD;
TRUNCATE TABLE DWH.NORMALIZED.DWH_AGG_APPEL_DISTANCE;
