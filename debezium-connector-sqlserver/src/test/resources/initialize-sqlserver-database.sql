CREATE DATABASE testDB
CREATE DATABASE testDB2
USE testDB2

-- Gives the SQL Server Agent time to start before applying CDC operations
-- If the Agent isn't running, a CDC operation will fail and the container won't start
WAITFOR DELAY '00:00:30'

EXEC sys.sp_cdc_enable_db

CREATE SCHEMA inventory

CREATE TABLE inventory.products (id INTEGER IDENTITY(101,1) NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(512), weight FLOAT)
INSERT INTO inventory.products(name,description,weight) VALUES ('scooter','Small 2-wheel scooter',3.14)
INSERT INTO inventory.products(name,description,weight) VALUES ('car battery','12V car battery',8.1)
INSERT INTO inventory.products(name,description,weight) VALUES ('12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8)
INSERT INTO inventory.products(name,description,weight) VALUES ('hammer','12oz carpenter''s hammer',0.75)
INSERT INTO inventory.products(name,description,weight) VALUES ('hammer','14oz carpenter''s hammer',0.875)
INSERT INTO inventory.products(name,description,weight) VALUES ('hammer','16oz carpenter''s hammer',1.0)
INSERT INTO inventory.products(name,description,weight) VALUES ('rocks','box of assorted rocks',5.3)
INSERT INTO inventory.products(name,description,weight) VALUES ('jacket','water resistent black wind breaker',0.1)
INSERT INTO inventory.products(name,description,weight) VALUES ('spare tire','24 inch spare tire',22.2)
EXEC sys.sp_cdc_enable_table @source_schema = 'inventory', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0

USE testDB

EXEC sys.sp_cdc_enable_db

CREATE SCHEMA inventory

CREATE TABLE inventory.products (id INTEGER IDENTITY(101,1) NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(512), weight FLOAT)
INSERT INTO inventory.products(name,description,weight) VALUES ('scooter','Small 2-wheel scooter',3.14)
INSERT INTO inventory.products(name,description,weight) VALUES ('car battery','12V car battery',8.1)
INSERT INTO inventory.products(name,description,weight) VALUES ('12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8)
INSERT INTO inventory.products(name,description,weight) VALUES ('hammer','12oz carpenter''s hammer',0.75)
INSERT INTO inventory.products(name,description,weight) VALUES ('hammer','14oz carpenter''s hammer',0.875)
INSERT INTO inventory.products(name,description,weight) VALUES ('hammer','16oz carpenter''s hammer',1.0)
INSERT INTO inventory.products(name,description,weight) VALUES ('rocks','box of assorted rocks',5.3)
INSERT INTO inventory.products(name,description,weight) VALUES ('jacket','water resistent black wind breaker',0.1)
INSERT INTO inventory.products(name,description,weight) VALUES ('spare tire','24 inch spare tire',22.2)
EXEC sys.sp_cdc_enable_table @source_schema = 'inventory', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0

CREATE TABLE inventory.products_on_hand (product_id INTEGER NOT NULL PRIMARY KEY, quantity INTEGER NOT NULL, FOREIGN KEY (product_id) REFERENCES inventory.products(id))
INSERT INTO inventory.products_on_hand VALUES (101,3)
INSERT INTO inventory.products_on_hand VALUES (102,8)
INSERT INTO inventory.products_on_hand VALUES (103,18)
INSERT INTO inventory.products_on_hand VALUES (104,4)
INSERT INTO inventory.products_on_hand VALUES (105,5)
INSERT INTO inventory.products_on_hand VALUES (106,0)
INSERT INTO inventory.products_on_hand VALUES (107,44)
INSERT INTO inventory.products_on_hand VALUES (108,2)
INSERT INTO inventory.products_on_hand VALUES (109,5)
EXEC sys.sp_cdc_enable_table @source_schema = 'inventory', @source_name = 'products_on_hand', @role_name = NULL, @supports_net_changes = 0

CREATE TABLE inventory.customers (id INTEGER IDENTITY(1001,1) NOT NULL PRIMARY KEY, first_name VARCHAR(255) NOT NULL, last_name VARCHAR(255) NOT NULL, email VARCHAR(255) NOT NULL UNIQUE)
INSERT INTO inventory.customers(first_name,last_name,email) VALUES ('Sally','Thomas','sally.thomas@acme.com')
INSERT INTO inventory.customers(first_name,last_name,email) VALUES ('George','Bailey','gbailey@foobar.com')
INSERT INTO inventory.customers(first_name,last_name,email) VALUES ('Edward','Walker','ed@walker.com')
INSERT INTO inventory.customers(first_name,last_name,email) VALUES ('Anne','Kretchmar','annek@noanswer.org')
EXEC sys.sp_cdc_enable_table @source_schema = 'inventory', @source_name = 'customers', @role_name = NULL, @supports_net_changes = 0

CREATE TABLE inventory.orders (id INTEGER IDENTITY(10001,1) NOT NULL PRIMARY KEY, order_date DATE NOT NULL, purchaser INTEGER NOT NULL, quantity INTEGER NOT NULL, product_id INTEGER NOT NULL, FOREIGN KEY (purchaser) REFERENCES inventory.customers(id), FOREIGN KEY (product_id) REFERENCES inventory.products(id))
INSERT INTO inventory.orders(order_date,purchaser,quantity,product_id) VALUES ('16-JAN-2016', 1001, 1, 102)
INSERT INTO inventory.orders(order_date,purchaser,quantity,product_id) VALUES ('17-JAN-2016', 1002, 2, 105)
INSERT INTO inventory.orders(order_date,purchaser,quantity,product_id) VALUES ('19-FEB-2016', 1002, 2, 106)
INSERT INTO inventory.orders(order_date,purchaser,quantity,product_id) VALUES ('21-FEB-2016', 1003, 1, 107)
EXEC sys.sp_cdc_enable_table @source_schema = 'inventory', @source_name = 'orders', @role_name = NULL, @supports_net_changes = 0
