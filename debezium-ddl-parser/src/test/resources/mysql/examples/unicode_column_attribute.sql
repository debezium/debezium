-- debezium/dbz#1272: UNICODE character set shorthand on a column
ALTER TABLE descriptors MODIFY Description VARCHAR(512) UNICODE;
CREATE TABLE t1 (a CHAR(10) UNICODE, b VARCHAR(20) UNICODE);
