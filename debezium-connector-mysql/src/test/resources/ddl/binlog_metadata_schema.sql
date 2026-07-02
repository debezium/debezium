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
