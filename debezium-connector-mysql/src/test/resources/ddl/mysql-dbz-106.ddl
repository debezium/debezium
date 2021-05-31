CREATE TABLE `DBZ106` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `YYYY` bigint(20) unsigned NOT NULL,
 `key` varchar(64) NOT NULL DEFAULT '',
 `value` text,
 `FFFFF` tinyint(4) DEFAULT '0',
 `DDDD` bigint(20) DEFAULT '0',
 `TTTT` binary(16) DEFAULT NULL,
 `BBB` text,
 PRIMARY KEY (`id`),
 KEY `TYTUT` (`risk_info_id`),
 KEY `BGFH` (`key`),
 KEY `HGJG` (`risk_info_id`,`create_time`),
 KEY `EEEEE` (`is_encrypted`),
 KEY `GGGGG` (`risk_info_id`,`context_uuid`),
 KEY `HGHGHG` (`context_uuid`),
 CONSTRAINT `GHGHGHGHG` FOREIGN KEY (`risk_info_id`) REFERENCES `HGHGHG` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10851 DEFAULT CHARSET=utf8;
