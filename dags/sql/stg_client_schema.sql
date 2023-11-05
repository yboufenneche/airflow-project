CREATE TABLE IF NOT EXISTS stg_client (
    Id_Client NUMERIC(10),
    Nom_Client VARCHAR(15),
    Prenom_Client VARCHAR(20),
    Numero VARCHAR(10),
    Id_Offre NUMERIC(3),
    Date_abonnement DATE,
    PRIMARY KEY (Id_Client)
);

TRUNCATE TABLE stg_client;