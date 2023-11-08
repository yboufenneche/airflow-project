CREATE TABLE IF NOT EXISTS src_produit (
    Id_Produit INT,
    Lib_Produit VARCHAR(8),
    Desc_Produit VARCHAR(20),
    CONSTRAINT src_produit_PK PRIMARY KEY (Id_Produit)
);

TRUNCATE TABLE src_produit;