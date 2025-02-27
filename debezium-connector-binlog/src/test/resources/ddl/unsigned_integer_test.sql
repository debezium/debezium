-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  unsigned_integer_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scan all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
--
-- This relies upon MySQL 5.7's Geometries datatypes.

-- DBZ-228 handle unsigned TINYINT UNSIGNED
CREATE TABLE dbz_228_tinyint_unsigned (
  id int auto_increment NOT NULL,
  c1 TINYINT(3) UNSIGNED ZEROFILL NOT NULL,
  c2 TINYINT(3) UNSIGNED NOT NULL,
  c3 TINYINT(3) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_228_tinyint_unsigned VALUES (default, 255, 255, 127);
INSERT INTO dbz_228_tinyint_unsigned VALUES (default, 155, 155, -100);
INSERT INTO dbz_228_tinyint_unsigned VALUES (default, 0, 0, -128);


-- DBZ-228 handle unsigned SMALLINT UNSIGNED
CREATE TABLE dbz_228_smallint_unsigned (
  id int auto_increment NOT NULL,
  c1 SMALLINT UNSIGNED ZEROFILL NOT NULL,
  c2 SMALLINT UNSIGNED NOT NULL,
  c3 SMALLINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_228_smallint_unsigned VALUES (default, 65535, 65535, 32767);
INSERT INTO dbz_228_smallint_unsigned VALUES (default, 45535, 45535, -12767);
INSERT INTO dbz_228_smallint_unsigned VALUES (default, 0, 0, -32768);


-- DBZ-228 handle unsigned MEDIUMINT UNSIGNED
CREATE TABLE dbz_228_mediumint_unsigned (
  id int auto_increment NOT NULL,
  c1 MEDIUMINT UNSIGNED ZEROFILL NOT NULL,
  c2 MEDIUMINT UNSIGNED NOT NULL,
  c3 MEDIUMINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_228_mediumint_unsigned VALUES (default, 16777215, 16777215, 8388607);
INSERT INTO dbz_228_mediumint_unsigned VALUES (default, 10777215, 10777215, -6388607);
INSERT INTO dbz_228_mediumint_unsigned VALUES (default, 0, 0, -8388608);

-- DBZ-228 handle unsigned INT UNSIGNED
CREATE TABLE dbz_228_int_unsigned (
  id int auto_increment NOT NULL,
  c1 int(11) UNSIGNED ZEROFILL NOT NULL,
  c2 int(11) UNSIGNED NOT NULL,
  c3 int(11) NOT NULL,
  c4 int(5) UNSIGNED ZEROFILL NOT NULL,
  c5 int(5) UNSIGNED NOT NULL ,
  c6 int(5) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_228_int_unsigned VALUES (default, 4294967295, 4294967295, 2147483647, 4294967295, 4294967295, 2147483647);
INSERT INTO dbz_228_int_unsigned VALUES (default, 3294967295, 3294967295, -1147483647, 3294967295, 3294967295, -1147483647);
INSERT INTO dbz_228_int_unsigned VALUES (default, 0, 0, -2147483648, 0, 0, -2147483648);


-- DBZ-228 handle unsigned BIGINT UNSIGNED
CREATE TABLE dbz_228_bigint_unsigned (
  id int auto_increment NOT NULL,
  c1 BIGINT UNSIGNED ZEROFILL NOT NULL,
  c2 BIGINT UNSIGNED NOT NULL,
  c3 BIGINT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_228_bigint_unsigned VALUES (default, 18446744073709551615, 18446744073709551615, 9223372036854775807);
INSERT INTO dbz_228_bigint_unsigned VALUES (default, 14446744073709551615, 14446744073709551615, -1223372036854775807);
INSERT INTO dbz_228_bigint_unsigned VALUES (default, 0, 0, -9223372036854775808);

-- DBZ-1185 handle SERIAL type alias
CREATE TABLE dbz_1185_serial (
  id SERIAL NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_1185_serial VALUES (10);
INSERT INTO dbz_1185_serial VALUES (default);
INSERT INTO dbz_1185_serial VALUES (18446744073709551615);

-- DBZ-1185 handle SERIAL default value
CREATE TABLE dbz_1185_serial_default_value (
  id SMALLINT UNSIGNED SERIAL DEFAULT VALUE NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO dbz_1185_serial_default_value VALUES (10);
INSERT INTO dbz_1185_serial_default_value VALUES (default);
INSERT INTO dbz_1185_serial_default_value VALUES (1000);