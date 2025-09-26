CREATE TABLE orders(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL
);

CREATE TABLE users(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO orders (id, name, description) VALUES (1, 'pizza', 'pizza with peperoni'), (2, 'kebab', 'kebab with mayonnaise');