CREATE TABLE `BOOLEAN_TEST` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `b1` boolean default true,
  `b2` boolean default false,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
INSERT INTO BOOLEAN_TEST(b1, b2) VALUE (false, true);