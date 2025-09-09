CREATE TABLE products(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL
);

CREATE TABLE users(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO products (id, name, description) VALUES (1, 't-shirt', 'red hat t-shirt'), (2, 'sweatshirt', 'blue ibm sweatshirt');
INSERT INTO users (name) VALUES ('alvar'), ('anisha'), ('chris'), ('indra'), ('jiri'), ('giovanni'), ('mario'), ('rené'), ('Vojtěch');