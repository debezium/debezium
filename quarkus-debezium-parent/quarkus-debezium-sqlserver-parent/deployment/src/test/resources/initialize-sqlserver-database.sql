CREATE DATABASE testDB
USE testDB

-- Gives the SQL Server Agent time to start before applying CDC operations
-- If the Agent isn't running, a CDC operation will fail and the container won't start
WAITFOR DELAY '00:00:30'

EXEC sys.sp_cdc_enable_db

CREATE SCHEMA inventory

-- expected data:
--  users:
--    | id | name | description |
--    | 1  | giovanni | developer |
--    | 2 | mario | developer |
--  orders:
--    | key | name |
--    | 1 | one |
--    | 2 | two |


CREATE TABLE inventory.products(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)
CREATE TABLE inventory.orders(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)
CREATE TABLE inventory.users(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL)

INSERT INTO inventory.orders (id, name) VALUES (1, 'one'), (2,'two')
INSERT INTO inventory.users (id, name, description) VALUES (1,'giovanni', 'developer'), (2,'mario', 'developer')
