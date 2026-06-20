-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  connector_noblob_mode_test
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our products using a single insert with many rows
CREATE TABLE Products (
    PRIMARY KEY (id),
    id INTEGER NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    description BLOB
) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO Products
VALUES (default,"scooter","Small 2-wheel scooter"),
    (default,"car battery","12V car battery"),
    (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3"),
    (default,"hammer","12oz carpenter's hammer"),
    (default,"hammer2","14oz carpenter's hammer"),
    (default,"hammer3","16oz carpenter's hammer"),
    (default,"rocks","box of assorted rocks"),
    (default,"jacket","water resistent black wind breaker"),
    (default,"spare tire","24 inch spare tire");
