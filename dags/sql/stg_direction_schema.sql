CREATE OR REPLACE TABLE stg_direction (
    "Id_Direction" NUMBER(2,0),
    "Lib_Direction" VARCHAR(15),
    CONSTRAINT SRC_DIRECTION_PK PRIMARY KEY ("Id_Direction")
);

TRUNCATE TABLE stg_direction;
