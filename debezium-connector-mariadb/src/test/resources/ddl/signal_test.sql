-- Test database initialization for MariaDbBinlogPositionSignalIT

CREATE TABLE test_table (
    id INT PRIMARY KEY,
    value VARCHAR(100)
);

CREATE TABLE debezium_signal (
    id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data TEXT NULL
);

-- Insert initial test data to populate the table before snapshot
INSERT INTO test_table VALUES (0, 'initial');
