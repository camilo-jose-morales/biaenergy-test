CREATE TABLE IF NOT EXISTS geo (
    id int NOT NULL,
    lat FLOAT,
    lon FLOAT,
    postcode varchar,
    CONSTRAINT id_pk PRIMARY KEY (id)

)