/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mariadbuser';

GRANT ALL PRIVILEGES ON *.* TO 'mariadbuser'@'%';

CREATE DATABASE alternative;

use alternative;


CREATE TABLE orders
(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(255) NOT NULL
);

INSERT INTO orders(id, name, description)
VALUES (1, 'pizza', 'pizza with peperoni'),
       (2, 'kebab', 'kebab with mayonnaise')