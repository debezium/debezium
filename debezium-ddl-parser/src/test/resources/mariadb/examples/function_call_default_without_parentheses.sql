-- debezium/dbz#68: MariaDB allows a bare function call as a column DEFAULT (MySQL requires parentheses)
ALTER TABLE product_indicator ADD uid VARCHAR(32) DEFAULT UUID_SHORT() NOT NULL COMMENT 'Unique product indicator identifier.' AFTER id;
CREATE TABLE t1 (id INT, uid VARCHAR(36) DEFAULT UUID());
