CREATE TABLE `DBZ123` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `Provider_ID` bigint(20) NOT NULL,
  `External_ID` varchar(255) NOT NULL,
  `Name` varchar(255) NOT NULL,
  `Is_Enabled` bit(1) NOT NULL DEFAULT b'1',
  `binaryRepresentation` BLOB NOT NULL DEFAULT x'cafe',
  `BonusFactor` decimal(19,8) NOT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `game_unq` (`Provider_ID`,`External_ID`)
) ENGINE=InnoDB AUTO_INCREMENT=2374 DEFAULT CHARSET=utf8