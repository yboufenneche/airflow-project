CREATE TABLE IF NOT EXISTS stg_offre (
    Id_Offre NUMERIC(3),
    Lib_Offre VARCHAR(15),
    Desc_Offre VARCHAR(30),
    PRIMARY KEY (Id_Offre)
);

TRUNCATE TABLE stg_offre;