-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  binary_column_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_1814_binary_mode_test (
      id INT AUTO_INCREMENT NOT NULL,
      blob_col BLOB NOT NULL,
      tinyblob_col TINYBLOB NOT NULL,
      mediumblob_col MEDIUMBLOB NOT NULL,
      longblob_col LONGBLOB NOT NULL,
      binary_col BINARY(3) NOT NULL,
      varbinary_col varbinary(20) NOT NULL,
      PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_1814_binary_mode_test (
    id,
    blob_col,
    tinyblob_col,
    mediumblob_col,
    longblob_col,
    binary_col,
    varbinary_col )
VALUES (
    default,
    X'010203',
    X'010203',
    X'010203',
    X'010203',
    X'010203',
    X'010203' );
