-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  regression_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scan all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.

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
  c1 enUM('a','b','c'),
  c2 Set('a','b','c')
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

-- DBZ-1318 handle zero-value dates in when zero dates allowed
CREATE TABLE dbz_1318_zerovaluetest (
  c1 DATE,
  c2 TIME(2),
  c3 DATETIME(2),
  c4 TIMESTAMP(2),
  nnc1 DATE NOT NULL,
  nnc2 TIME(2) NOT NULL,
  nnc3 DATETIME(2) NOT NULL
);
set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
INSERT IGNORE INTO dbz_1318_zerovaluetest VALUES ('0000-00-00', '00:00:00.000', '0000-00-00 00:00:00.000', '0000-00-00 00:00:00.000', '0000-00-00', '00:00:00.000', '0000-00-00 00:00:00.000');
INSERT IGNORE INTO dbz_1318_zerovaluetest VALUES ('0001-00-00', '00:01:00.000', '0000-00-00 14:02:10.000', '0000-00-00 14:02:10.000', '0001-00-00', '00:01:00.000', '0000-00-00 14:02:10.000');
set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

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
DROP DATABASE IF EXISTS connector_test;
CREATE DATABASE connector_test;
CREATE TABLE connector_test.customers (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT=1001;
INSERT INTO connector_test.customers
VALUES (default,"Sally","Thomas","sally.thomas@acme.com"),
       (default,"George","Bailey","gbailey@foobar.com"),
       (default,"Edward","Walker","ed@walker.com"),
       (default,"Anne","Kretchmar","annek@noanswer.org");

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

-- DBZ-162 handle function declarations with newline characters
CREATE FUNCTION fnDbz162( p_creditLimit DOUBLE ) RETURNS VARCHAR(10)
    DETERMINISTIC
BEGIN
 DECLARE lvl VARCHAR(10)$$
 IF p_creditLimit > 50000 THEN
   SET lvl = 'PLATINUM'$$
 ELSEIF (p_creditLimit <= 50000 AND p_creditLimit >= 10000) THEN
   SET lvl = 'GOLD'$$
 ELSEIF p_creditLimit < 10000 THEN
   SET lvl = 'SILVER'$$
 END IF$$
 RETURN (lvl)$$
END$$
;

-- DBZ-195 handle numeric values
CREATE TABLE dbz_195_numvalues (
  id int auto_increment NOT NULL,
  `search_version_read` int(11) NOT NULL DEFAULT '0', -- (11) is the display width 
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4972 DEFAULT CHARSET=utf8;

INSERT INTO dbz_195_numvalues VALUES (default,0);
INSERT INTO dbz_195_numvalues VALUES (default,-2147483648);
INSERT INTO dbz_195_numvalues VALUES (default,2147483647);

-- DBZ-342 handle TIME values that exceed the value range of java.sql.Time
CREATE TABLE dbz_342_timetest (
  c1 TIME(2),
  c2 TIME(0),
  c3 TIME(3),
  c4 TIME(3),
  c5 TIME(6),
  c6 TIME(6),
  c7 TIME(6),
  c8 TIME(6),
  c9 TIME(6),
  c10 TIME(6),
  c11 TIME(6)
);
INSERT INTO dbz_342_timetest
VALUES (
           '517:51:04.777',
           '-13:14:50',
           '-733:00:00.0011',
           '-1:59:59.0011',
           '-838:59:58.999999',
           '-00:20:38.000000', -- DBZ-7594
           '-01:01:01.000001', -- DBZ-7594
           '-01:01:01.000000', -- DBZ-7594
           '-01:01:00.000000', -- DBZ-7594
           '-01:00:00.000000', -- DBZ-7594
           '-00:00:00.000000'  -- DBZ-7594
       );
