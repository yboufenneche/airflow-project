--- Postgres
\c src;

CREATE TABLE src_direction (
    Id_Direction SMALLINT,
    Lib_Direction VARCHAR(30),
    CONSTRAINT SRC_DIRECTION_PK PRIMARY KEY (Id_Direction)
);

TRUNCATE TABLE src_direction;