-- debezium/dbz#3080: empty hexadecimal literal X'' (zero-length binary string), accepted by MariaDB.
CREATE TABLE t_default (v VARBINARY(1) DEFAULT X'');
ALTER TABLE t_default ADD COLUMN normalized_ref VARBINARY(16) GENERATED ALWAYS AS (IF(v = 'default', X'', v)) VIRTUAL;
CREATE TABLE t_control (v VARBINARY(1) DEFAULT X'00');
