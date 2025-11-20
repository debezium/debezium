-- Test database initialization for MySqlBinlogPositionSignalIT

CREATE TABLE test_table (
    id INT PRIMARY KEY,
    value VARCHAR(100)
);

CREATE TABLE debezium_signal (
    id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data TEXT NULL
);
