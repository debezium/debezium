-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  source_type_as_schema_parameter_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_644_source_type_mapped_as_schema_parameter_test (
      id INT AUTO_INCREMENT NOT NULL,
      c1 INT,
      c2 MEDIUMINT,
      c3a NUMERIC(5,2),
      c3b VARCHAR(128),
      f1 FLOAT(10),
      f2 FLOAT(8,4),
      PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_644_source_type_mapped_as_schema_parameter_test VALUES (default, 123, 456, 789.01, 'test', 1.228, 234.56);
