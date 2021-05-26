-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE: topic-name-sanitization-it
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE `dbz_878_some|test@data` (
      id INT,
      some_col VARCHAR(255),
      PRIMARY KEY (id)
  ) DEFAULT CHARSET=utf8;

INSERT INTO `dbz_878_some|test@data` VALUES (1, 'some text');

CREATE TABLE `DBZ.1834` (
      id INT,
      some_col VARCHAR(255),
      PRIMARY KEY (id)
  ) DEFAULT CHARSET=utf8;

INSERT INTO `DBZ.1834` VALUES (1, 'some text');
