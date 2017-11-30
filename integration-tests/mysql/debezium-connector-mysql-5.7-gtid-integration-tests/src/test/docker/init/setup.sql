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

-- DBZ-85 handle fractional part of seconds
CREATE TABLE dbz_85_fractest (
  c1 DATE,
  c2 TIME(2),
  c3 DATETIME(2),
  c4 TIMESTAMP(2)
);
INSERT INTO dbz_85_fractest VALUES ('2014-09-08', '17:51:04.777', '2014-09-08 17:51:04.777', '2014-09-08 17:51:04.777');

-- DBZ-100 handle enum and set
CREATE TABLE dbz_100_enumsettest (
  c1 ENUM('a','b','c'),
  c2 SET('a','b','c')
);
INSERT INTO dbz_100_enumsettest VALUES ('a', 'a,b,c');
INSERT INTO dbz_100_enumsettest VALUES ('b', 'b,a');
INSERT INTO dbz_100_enumsettest VALUES ('c', 'a');

-- DBZ-102 handle character sets
-- Use session variables to dictate the character sets used by the client running these commands so
-- the literal value is interpretted correctly...
set character_set_client=utf8;
set character_set_connection=utf8;
CREATE TABLE dbz_102_charsettest (
  id INT(11) NOT NULL AUTO_INCREMENT,
  text VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2001 DEFAULT CHARSET=utf8;
INSERT INTO dbz_102_charsettest VALUES (default, "产品");

-- DBZ-114 handle zero-value dates
CREATE TABLE dbz_114_zerovaluetest (
  c1 DATE,
  c2 TIME(2),
  c3 DATETIME(2),
  c4 TIMESTAMP(2)
);
INSERT IGNORE INTO dbz_114_zerovaluetest VALUES ('0000-00-00', '00:00:00.000', '0000-00-00 00:00:00.000', '0000-00-00 00:00:00.000');
INSERT IGNORE INTO dbz_114_zerovaluetest VALUES ('0001-00-00', '00:01:00.000', '0001-00-00 00:00:00.000', '0001-00-00 00:00:00.000');


-- DBZ-123 handle bit values, including bit field literals
CREATE TABLE dbz_123_bitvaluetest (
  c1 BIT,
  c2 BIT(2),
  c3 BIT(8) NOT NULL,
  c4 BIT(64)
);
INSERT INTO dbz_123_bitvaluetest VALUES (1,2,64,23989979);
INSERT INTO dbz_123_bitvaluetest VALUES (b'1',b'10',b'01000000',b'1011011100000111011011011');

-- DBZ-104 handle create table like ...
CREATE TABLE dbz_104_customers LIKE connector_test.customers;
INSERT INTO dbz_104_customers SELECT * FROM connector_test.customers;

-- DBZ-147 handle decimal value
CREATE TABLE dbz_147_decimalvalues (
  pk_column int auto_increment NOT NULL,
  decimal_value decimal(7,2) NOT NULL,
  PRIMARY KEY(pk_column)
);
INSERT INTO dbz_147_decimalvalues (pk_column, decimal_value)
VALUES(default, 12345.67);


-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  json_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scans all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
--
-- This relies upon MySQL 5.7's JSON datatype.
CREATE DATABASE json_test;
USE json_test;

-- DBZ-126 handle JSON column types ...
CREATE TABLE dbz_126_jsontable (
  id INT AUTO_INCREMENT NOT NULL,
  json JSON,
  expectedJdbcStr VARCHAR(256), -- value that we get back from JDBC
  expectedBinlogStr VARCHAR(256), -- value we parse from the binlog
  PRIMARY KEY(id)
) DEFAULT CHARSET=utf8;
INSERT INTO dbz_126_jsontable VALUES (default,NULL,
                                              NULL,
                                              NULL);
INSERT INTO dbz_126_jsontable VALUES (default,'{"a": 2}',
                                              '{"a": 2}',
                                              '{"a":2}');
INSERT INTO dbz_126_jsontable VALUES (default,'[1, 2]',
                                              '[1, 2]',
                                              '[1,2]');
INSERT INTO dbz_126_jsontable VALUES (default,'{"key1": "value1", "key2": "value2"}',
                                              '{"key1": "value1", "key2": "value2"}',
                                              '{"key1":"value1","key2":"value2"}');
INSERT INTO dbz_126_jsontable VALUES (default,'["a", "b",1]',
                                              '["a", "b",1]',
                                              '["a","b",1]');
INSERT INTO dbz_126_jsontable VALUES (default,'{"k1": "v1", "k2": {"k21": "v21", "k22": "v22"}, "k3": ["a", "b", 1]}',
                                              '{"k1": "v1", "k2": {"k21": "v21", "k22": "v22"}, "k3": ["a", "b", 1]}',
                                              '{"k1":"v1","k2":{"k21":"v21","k22":"v22"},"k3":["a","b",1]}');
INSERT INTO dbz_126_jsontable VALUES (default,'{"a": "b", "c": "d", "ab": "abc", "bc": ["x", "y"]}',
                                              '{"a": "b", "c": "d", "ab": "abc", "bc": ["x", "y"]}',
                                              '{"a":"b","c":"d","ab":"abc","bc":["x","y"]}');
INSERT INTO dbz_126_jsontable VALUES (default,'["here", ["I", "am"], "!!!"]',
                                              '["here", ["I", "am"], "!!!"]',
                                              '["here",["I","am"],"!!!"]');
INSERT INTO dbz_126_jsontable VALUES (default,'"scalar string"',
                                              '"scalar string"',
                                              '"scalar string"');
INSERT INTO dbz_126_jsontable VALUES (default,'true',
                                              'true',
                                              'true');
INSERT INTO dbz_126_jsontable VALUES (default,'false',
                                              'false',
                                              'false');
INSERT INTO dbz_126_jsontable VALUES (default,'null',
                                              'null',
                                              'null');
INSERT INTO dbz_126_jsontable VALUES (default,'-1',
                                              '-1',
                                              '-1');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST(1 AS UNSIGNED) AS JSON),
                                              '1',
                                              '1');
INSERT INTO dbz_126_jsontable VALUES (default,'32767',
                                              '32767',
                                              '32767');
INSERT INTO dbz_126_jsontable VALUES (default,'32768',
                                              '32768',
                                              '32768');
INSERT INTO dbz_126_jsontable VALUES (default,'-32768',
                                              '-32768',
                                              '-32768');
INSERT INTO dbz_126_jsontable VALUES (default,'2147483647', -- INT32
                                              '2147483647',
                                              '2147483647');
INSERT INTO dbz_126_jsontable VALUES (default,'2147483648', -- INT64
                                              '2147483648',
                                              '2147483648');
INSERT INTO dbz_126_jsontable VALUES (default,'-2147483648', -- INT32
                                              '-2147483648',
                                              '-2147483648');
INSERT INTO dbz_126_jsontable VALUES (default,'-2147483649', -- INT64
                                              '-2147483649',
                                              '-2147483649');
INSERT INTO dbz_126_jsontable VALUES (default,'18446744073709551615', -- INT64
                                              '18446744073709551615',
                                              '18446744073709551615');
INSERT INTO dbz_126_jsontable VALUES (default,'18446744073709551616', -- BigInteger
                                              '18446744073709551616',
                                              '18446744073709551616');
INSERT INTO dbz_126_jsontable VALUES (default,'3.14',
                                              '3.14',
                                              '3.14');
INSERT INTO dbz_126_jsontable VALUES (default,'{}',
                                              '{}',
                                              '{}');
INSERT INTO dbz_126_jsontable VALUES (default,'[]',
                                              '[]',
                                              '[]');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON),
                                              '"2015-01-15 23:24:25"',
                                              '"2015-01-15 23:24:25"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('2015-01-15 23:24:25.12' AS DATETIME(3)) AS JSON),
                                              '"2015-01-15 23:24:25.12"',
                                              '"2015-01-15 23:24:25.12"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('2015-01-15 23:24:25.0237' AS DATETIME(3)) AS JSON),
                                              '"2015-01-15 23:24:25.024"',
                                              '"2015-01-15 23:24:25.024"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('23:24:25' AS TIME) AS JSON),
                                              '"23:24:25"',
                                              '"23:24:25"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON),
                                              '"23:24:25.12"',
                                              '"23:24:25.12"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON),
                                              '"23:24:25.024"',
                                              '"23:24:25.024"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(CAST('2015-01-15' AS DATE) AS JSON),
                                              '"2015-01-15"',
                                              '"2015-01-15"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON),
                                              '"2015-01-15 23:24:25"',
                                              '"2015-01-15 23:24:25"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON),
                                              '"2015-01-15 23:24:25.12"',
                                              '"2015-01-15 23:24:25.12"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON),
                                              '"2015-01-15 23:24:25.0237"',
                                              '"2015-01-15 23:24:25.0237"');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(UNIX_TIMESTAMP('2015-01-15 23:24:25') AS JSON),
                                              '1421364265',
                                              '1421364265');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(ST_GeomFromText('POINT(1 1)') AS JSON),
                                              '{\"type\": \"Point\", \"coordinates\": [1.0, 1.0]}',
                                              '{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}');
INSERT INTO dbz_126_jsontable VALUES (default,CAST('[]' AS CHAR CHARACTER SET 'ascii'),
                                              '[]',
                                              '[]');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(x'cafe' AS JSON), -- BLOB as Base64
                                              '"yv4="',
                                              '"yv4="');
INSERT INTO dbz_126_jsontable VALUES (default,CAST(x'cafebabe' AS JSON), -- BLOB as Base64
                                              '"yv66vg=="',
                                              '"yv66vg=="');

-- DBZ-342 handle TIME values that exceed the value range of java.sql.Time
CREATE TABLE dbz_342_timetest (
  c1 TIME(2),
  c2 TIME(0),
  c3 TIME(3),
  c4 TIME(3),
  c5 TIME(6)
);
INSERT INTO dbz_342_timetest VALUES ('517:51:04.777', '-13:14:50', '-733:00:00.0011', '-1:59:59.0011', '-838:59:58.999999');
