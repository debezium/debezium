-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant 3 users different privileges:
--
-- 1) 'replicator' - all privileges required by the binlog reader (setup through 'readbinlog.sql')
-- 2) 'snapper' - all privileges required by the snapshot reader AND binlog reader
-- 3) 'mysqluser' - all privileges
--
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator' IDENTIFIED BY 'replpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'snapper'@'%' IDENTIFIED BY 'snapperpass';
GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%';

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  emptydb
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE emptydb;

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  readbinlog_test
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE readbinlog_test;

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  connector_test
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE connector_test;
USE connector_test;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight FLOAT
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default,"scooter","Small 2-wheel scooter",3.14),
       (default,"car battery","12V car battery",8.1),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (default,"hammer","12oz carpenter's hammer",0.75),
       (default,"hammer","14oz carpenter's hammer",0.875),
       (default,"hammer","16oz carpenter's hammer",1.0),
       (default,"rocks","box of assorted rocks",5.3),
       (default,"jacket","water resistent black wind breaker",0.1),
       (default,"spare tire","24 inch spare tire",22.2);

-- Create and populate the products on hand using multiple inserts
CREATE TABLE products_on_hand (
  product_id INTEGER NOT NULL PRIMARY KEY,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);

INSERT INTO products_on_hand VALUES (101,3);
INSERT INTO products_on_hand VALUES (102,8);
INSERT INTO products_on_hand VALUES (103,18);
INSERT INTO products_on_hand VALUES (104,4);
INSERT INTO products_on_hand VALUES (105,5);
INSERT INTO products_on_hand VALUES (106,0);
INSERT INTO products_on_hand VALUES (107,44);
INSERT INTO products_on_hand VALUES (108,2);
INSERT INTO products_on_hand VALUES (109,5);

-- Create some customers ...
CREATE TABLE customers (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT=1001;


INSERT INTO customers
VALUES (default,"Sally","Thomas","sally.thomas@acme.com"),
       (default,"George","Bailey","gbailey@foobar.com"),
       (default,"Edward","Walker","ed@walker.com"),
       (default,"Anne","Kretchmar","annek@noanswer.org");

-- Create some veyr simple orders
CREATE TABLE orders (
  order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_date DATE NOT NULL,
  purchaser INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
  FOREIGN KEY ordered_product (product_id) REFERENCES products(id)
) AUTO_INCREMENT = 10001;

INSERT INTO orders
VALUES (default, '2016-01-16', 1001, 1, 102),
       (default, '2016-01-17', 1002, 2, 105),
       (default, '2016-02-18', 1004, 3, 109),
       (default, '2016-02-19', 1002, 2, 106),
       (default, '2016-02-21', 1003, 1, 107);


-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  connector_test_ro
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE connector_test_ro;
USE connector_test_ro;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight FLOAT
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default,"scooter","Small 2-wheel scooter",3.14),
       (default,"car battery","12V car battery",8.1),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (default,"hammer","12oz carpenter's hammer",0.75),
       (default,"hammer","14oz carpenter's hammer",0.875),
       (default,"hammer","16oz carpenter's hammer",1.0),
       (default,"rocks","box of assorted rocks",5.3),
       (default,"jacket","water resistent black wind breaker",0.1),
       (default,"spare tire","24 inch spare tire",22.2);

-- Create and populate the products on hand using multiple inserts
CREATE TABLE products_on_hand (
  product_id INTEGER NOT NULL PRIMARY KEY,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);

INSERT INTO products_on_hand VALUES (101,3);
INSERT INTO products_on_hand VALUES (102,8);
INSERT INTO products_on_hand VALUES (103,18);
INSERT INTO products_on_hand VALUES (104,4);
INSERT INTO products_on_hand VALUES (105,5);
INSERT INTO products_on_hand VALUES (106,0);
INSERT INTO products_on_hand VALUES (107,44);
INSERT INTO products_on_hand VALUES (108,2);
INSERT INTO products_on_hand VALUES (109,5);

-- Create some customers ...
CREATE TABLE customers (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT=1001;


INSERT INTO customers
VALUES (default,"Sally","Thomas","sally.thomas@acme.com"),
       (default,"George","Bailey","gbailey@foobar.com"),
       (default,"Edward","Walker","ed@walker.com"),
       (default,"Anne","Kretchmar","annek@noanswer.org");

-- Create some veyr simple orders
CREATE TABLE orders (
  order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_date DATE NOT NULL,
  purchaser INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
  FOREIGN KEY ordered_product (product_id) REFERENCES products(id)
) AUTO_INCREMENT = 10001;

INSERT INTO orders
VALUES (default, '2016-01-16', 1001, 1, 102),
       (default, '2016-01-17', 1002, 2, 105),
       (default, '2016-02-18', 1004, 3, 109),
       (default, '2016-02-19', 1002, 2, 106),
       (default, '2016-02-21', 1003, 1, 107);



-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  regression_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scans all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
#
CREATE DATABASE regression_test;
USE regression_test;

-- DBZ-61 handle binary value recorded as hex string value
CREATE TABLE t1464075356413_testtable6 (
pk_column int auto_increment NOT NULL,
varbinary_col varbinary(20) NOT NULL,
PRIMARY KEY(pk_column)
);
INSERT INTO t1464075356413_testtable6 (pk_column, varbinary_col)
VALUES(default, 0x4D7953514C);

-- DBZ-84 Handle TINYINT
CREATE TABLE dbz84_integer_types_table (
-- The column lengths are used for display purposes, and do not affect the range of values
colTinyIntA tinyint NOT NULL DEFAULT 100,
colTinyIntB tinyint(1) NOT NULL DEFAULT 101,
colTinyIntC tinyint(2) UNSIGNED NOT NULL DEFAULT 102,
colTinyIntD tinyint(3) UNSIGNED NOT NULL DEFAULT 103,
colSmallIntA smallint NOT NULL DEFAULT 200,
colSmallIntB smallint(1) NOT NULL DEFAULT 201,
colSmallIntC smallint(2) NOT NULL DEFAULT 201,
colSmallIntD smallint(3) NOT NULL DEFAULT 201,
colMediumIntA mediumint NOT NULL DEFAULT 300,
colMediumIntB mediumint(1) NOT NULL DEFAULT 301,
colMediumIntC mediumint(2) NOT NULL DEFAULT 302,
colMediumIntD mediumint(3) NOT NULL DEFAULT 303,
colIntA int NOT NULL DEFAULT 400,
colIntB int(1) NOT NULL DEFAULT 401,
colIntC int(2) NOT NULL DEFAULT 402,
colIntD int(3) NOT NULL DEFAULT 403,
colBigIntA bigint NOT NULL DEFAULT 500,
colBigIntB bigint(1) NOT NULL DEFAULT 501,
colBigIntC bigint(2) NOT NULL DEFAULT 502,
colBigIntD bigint(3) NOT NULL DEFAULT 503
);
INSERT INTO dbz84_integer_types_table
VALUES(127,-128,128,255, default,201,202,203, default,301,302,303, default,401,402,403, default,501,502,503);
