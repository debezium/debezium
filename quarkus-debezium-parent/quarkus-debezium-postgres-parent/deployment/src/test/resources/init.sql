CREATE TABLE products(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE orders(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE users(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE not_assigned(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE injected(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE convert_table(
    id SERIAL PRIMARY KEY,
    num FLOAT NOT NULL
);

INSERT INTO products (id, name) VALUES (1, 't-shirt'), (2, 'smartphone');
INSERT INTO orders (id, name) VALUES (1, 'one'), (2,'two');
INSERT INTO users (name) VALUES ('giovanni'), ('mario');
INSERT INTO not_assigned (name) VALUES ('something'), ('should'), ('happens');
INSERT INTO injected (name) VALUES ('called'), ('without'), ('injecting');
INSERT INTO injected (name) VALUES ('called'), ('without'), ('injecting');
INSERT INTO convert_table (num) VALUES (1), (2), (3);