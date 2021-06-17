CREATE DATABASE captured;
CREATE DATABASE non_captured;

CREATE TABLE captured.ct (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT 'order code',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT 'order index',
  KEY `merchant_id_index` (`merchant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='captured table';

CREATE TABLE captured.nct (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT 'order code',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT 'order index',
  KEY `merchant_id_index` (`merchant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='non-captured table';

CREATE TABLE non_captured.nct (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT 'order code',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT 'order index',
  KEY `merchant_id_index` (`merchant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='non-captured table';
