CREATE TABLE IF NOT EXISTS stg_appel (
    Id_Client NUMERIC(10),
    Date_Appel DATE,
    Heure_Appel VARCHAR(5),
    No_Appelant VARCHAR(20),
    No_appele VARCHAR(20),
    Id_Direction NUMERIC(1),
    Id_Produit NUMERIC(1),
    Id_Distance NUMERIC(2),
    Durée NUMERIC(5),
    PRIMARY KEY (Date_Appel, Heure_Appel)
);

TRUNCATE TABLE stg_appel;