/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'mariadbuser';

CREATE DATABASE native;
GRANT ALL PRIVILEGES ON native.* TO 'mariadbuser'@'%';

use native;


CREATE TABLE users (
                       id INT AUTO_INCREMENT PRIMARY KEY,
                       name VARCHAR(255) NOT NULL
);

CREATE TABLE products (
                          id INT NOT NULL PRIMARY KEY,
                          name VARCHAR(255) NOT NULL,
                          description VARCHAR(255) NOT NULL
);

INSERT INTO users (name) VALUES ('alvar'), ('anisha'), ('chris'), ('indra'), ('jiri'), ('giovanni'), ('mario'), ('rené'), ('Vojtech');
INSERT INTO products (id, name, description) VALUES (1, 't-shirt','red hat t-shirt'), (2,'sweatshirt','blue ibm sweatshirt');