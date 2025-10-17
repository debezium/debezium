CREATE DATABASE inventory
USE inventory

WAITFOR DELAY '00:00:30'

EXEC sys.sp_cdc_enable_db


CREATE TABLE products(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)
CREATE TABLE general_table(id INT NOT NULL PRIMARY KEY)
CREATE TABLE orders(id INT NOT NULL PRIMARY KEY, [key] INT NOT NULL, name VARCHAR(255) NOT NULL)
CREATE TABLE users(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL)

INSERT INTO general_table(id) VALUES (1)
INSERT INTO orders (id,[key], name) VALUES (1,1, 'one'), (2,2,'two')
INSERT INTO users (id, name, description) VALUES (1,'giovanni', 'developer'), (2,'mario', 'developer')
INSERT INTO products (id, name) VALUES (1,'t-shirt'), (2,'thinkpad')

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL, @supports_net_changes = 0
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'users', @role_name = NULL, @supports_net_changes = 0
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'general_table', @role_name = NULL, @supports_net_changes = 0