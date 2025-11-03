USE inventory;

CREATE TABLE products(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);
CREATE TABLE general_table(id INT NOT NULL PRIMARY KEY);
CREATE TABLE orders(id INT NOT NULL PRIMARY KEY, `key` INT NOT NULL, name VARCHAR(255) NOT NULL);
CREATE TABLE users(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL);

INSERT INTO general_table(id) VALUES (1);
INSERT INTO orders (id, `key`, name) VALUES (1,1, 'one'), (2,2,'two');
INSERT INTO users (id, name, description) VALUES (1,'giovanni', 'developer'), (2,'mario', 'developer');
INSERT INTO products (id, name) VALUES (1,'t-shirt'), (2,'thinkpad');

