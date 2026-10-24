-- Table used by BinlogMetadataBasedSchemaIT (MySQL and MariaDB) to verify that the streaming schema
-- can be reconstructed purely from binlog TABLE_MAP metadata (binlog_row_metadata=FULL), without
-- relying on a persisted schema history topic. Rows are inserted at runtime by the test.
CREATE TABLE orders (
  id INT NOT NULL AUTO_INCREMENT,
  quantity BIGINT UNSIGNED,
  status ENUM('NEW','SHIPPED','CANCELLED') NOT NULL DEFAULT 'NEW',
  price DECIMAL(12,3),
  code CHAR(4) NOT NULL,
  note VARCHAR(64),
  created_at DATETIME(3),
  PRIMARY KEY (id)
) DEFAULT CHARSET=utf8mb4;

-- Broad data-type coverage: every column type must be reconstructed from TABLE_MAP metadata and the
-- row must decode without error. Types available on both MySQL 8.0+ and MariaDB 10.5+.
CREATE TABLE all_types (
  id            INT NOT NULL AUTO_INCREMENT,
  c_tinyint     TINYINT,
  c_tinyint_u   TINYINT UNSIGNED,
  c_bool        TINYINT(1),
  c_smallint    SMALLINT,
  c_smallint_u  SMALLINT UNSIGNED,
  c_mediumint   MEDIUMINT,
  c_mediumint_u MEDIUMINT UNSIGNED,
  c_int         INT,
  c_int_u       INT UNSIGNED,
  c_bigint      BIGINT,
  c_bigint_u    BIGINT UNSIGNED,
  c_decimal     DECIMAL(12,3),
  c_float       FLOAT,
  c_double      DOUBLE,
  c_bit         BIT(5),
  c_date        DATE,
  c_datetime    DATETIME(3),
  c_timestamp   TIMESTAMP(6) NULL,
  c_time        TIME(3),
  c_year        YEAR,
  c_char        CHAR(4),
  c_varchar     VARCHAR(64),
  c_binary      BINARY(8),
  c_varbinary   VARBINARY(32),
  c_tinytext    TINYTEXT,
  c_text        TEXT,
  c_mediumtext  MEDIUMTEXT,
  c_longtext    LONGTEXT,
  c_tinyblob    TINYBLOB,
  c_blob        BLOB,
  c_mediumblob  MEDIUMBLOB,
  c_longblob    LONGBLOB,
  c_enum        ENUM('NEW','OK','ERR') NOT NULL DEFAULT 'NEW',
  c_set         SET('a','b','c'),
  c_json        JSON,
  PRIMARY KEY (id)
) DEFAULT CHARSET=utf8mb4;

-- Mixed-charset table with the ENUM in front of the string columns: the ENUM takes its charset from
-- the ENUM_AND_SET_* metadata fields and must not shift the per-column charset numbering of the
-- string columns that follow it.
CREATE TABLE charset_mix (
  id INT NOT NULL AUTO_INCREMENT,
  status ENUM('NEW','DONE') NOT NULL,
  name_latin1 VARCHAR(80) CHARACTER SET latin1,
  note_utf8 VARCHAR(40) CHARACTER SET utf8mb4,
  PRIMARY KEY (id)
) DEFAULT CHARSET=utf8mb4;

-- The primary key uses a column prefix, so the server writes PRIMARY_KEY_WITH_PREFIX instead of
-- SIMPLE_PRIMARY_KEY into the TABLE_MAP metadata.
CREATE TABLE prefix_pk (
  code VARCHAR(64) NOT NULL,
  qty INT,
  PRIMARY KEY (code(5))
) DEFAULT CHARSET=utf8mb4;
