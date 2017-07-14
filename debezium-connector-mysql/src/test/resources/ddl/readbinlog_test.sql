-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  readbinlog_test
-- Database needs to be populated to break dependency between MetadataIT and MySqlConnectorIT.shouldValidateAcceptableConfiguration run order
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE person (
  name VARCHAR(255) primary key,
  birthdate DATE NULL,
  age INTEGER NULL DEFAULT 10,
  salary DECIMAL(5,2), bitStr BIT(18)
);
CREATE TABLE product (
  id INT NOT NULL AUTO_INCREMENT,
  createdByDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modifiedDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY(id)
);
CREATE TABLE purchased (
  purchaser VARCHAR(255) NOT NULL,
  productId INT NOT NULL,
  purchaseDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(productId,purchaser)
);
