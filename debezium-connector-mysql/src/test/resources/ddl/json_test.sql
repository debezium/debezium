-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  json_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scan all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
--
-- This relies upon MySQL 5.7's JSON datatype.

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