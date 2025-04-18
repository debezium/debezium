-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  converting_failure
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz7143 (
     id INT NOT NULL,
     age INT NULL,
     name VARCHAR(255) NULL,
     PRIMARY KEY(id)
);

CREATE TABLE time_table (
    id INT NOT NULL,
    A TIME(1) NULL,
    B TIME(6) NULL,
    C TIME(1) NOT NULL,
    PRIMARY KEY(id)
);