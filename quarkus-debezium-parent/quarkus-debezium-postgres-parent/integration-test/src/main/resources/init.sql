CREATE TABLE users(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO users (name) VALUES ('alvar'), ('anisha'), ('chris'), ('indra'), ('jiri'), ('giovanni'), ('mario'), ('rené'), ('Vojtěch');