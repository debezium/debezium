-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  binary_column_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_254_binary_column_test (
      id INT AUTO_INCREMENT NOT NULL,
      file_uuid BINARY(16),
      PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_254_binary_column_test VALUES (default, unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400','-','')));
INSERT INTO dbz_254_binary_column_test VALUES (default, unhex(replace('651aed08-390f-4893-b2f1-36923e7b74ab','-','')));
INSERT INTO dbz_254_binary_column_test VALUES (default, unhex(replace('651aed08-390f-4893-b2f1-36923e7b74','-','')));
INSERT INTO dbz_254_binary_column_test VALUES (default, unhex(00));