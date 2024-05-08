-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE: table_column_comment_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_4000_comment_test (
    id INT AUTO_INCREMENT NOT NULL COMMENT 'pk',
    name VARCHAR(255) NOT NULL COMMENT 'this is name column',
    value BIGINT NULL COMMENT 'the value is bigint type',
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8 COMMENT='table for dbz-4000';

INSERT INTO dbz_4000_comment_test VALUES (default, 'DBZ-4000', 4000);