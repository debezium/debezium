CREATE TABLE `NC_TEST` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `nc1` nchar default null,
  `nc2` nchar(5) default null,
  `nc3` nvarchar(25) default null,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
INSERT INTO NC_TEST(nc1,nc2,nc3) VALUES ('a', '123', 'hello');