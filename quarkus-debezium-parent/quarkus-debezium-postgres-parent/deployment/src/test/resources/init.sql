CREATE TABLE product(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO product (name) VALUES ('t-shirt'), ('smartphone');