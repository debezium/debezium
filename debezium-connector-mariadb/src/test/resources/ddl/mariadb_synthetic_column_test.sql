-- Table with a virtual generated column in a unique key.
-- MariaDB 10.2.1+ automatically creates a synthetic DB_ROW_HASH column to support
-- the unique index on a virtual column. This table is used to verify DBZ-1395.
CREATE TABLE dbz1395 (
    id INT NOT NULL PRIMARY KEY,
    text_val VARCHAR(255),
    v_col VARCHAR(255) GENERATED ALWAYS AS (UPPER(text_val)) VIRTUAL,
    UNIQUE KEY (v_col)
);
