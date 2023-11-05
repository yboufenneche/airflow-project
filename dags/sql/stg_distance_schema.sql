CREATE TABLE IF NOT EXISTS stg_distance (
    Id_Distance INT,
    Lib_Distance VARCHAR(50),
    Desc_Distance VARCHAR(50),
    CONSTRAINT SRC_REFDISTANCE_PK PRIMARY KEY (Id_Distance)
);

TRUNCATE TABLE stg_distance;