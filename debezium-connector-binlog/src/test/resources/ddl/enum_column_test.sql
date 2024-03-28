CREATE TABLE `test_stations_10` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(500) COLLATE utf8_unicode_ci NOT NULL,
    `type` enum('station', 'post_office') COLLATE utf8_unicode_ci NOT NULL DEFAULT 'station',
    `created` datetime DEFAULT CURRENT_TIMESTAMP,
    `modified` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
);

INSERT INTO test_stations_10 (`name`, `type`) values ( 'ha Tinh 7', 'station' );

ALTER TABLE `test_stations_10`
    MODIFY COLUMN `type` ENUM('station', 'post_office', 'plane', 'ahihi_dongok', 'now', 'test', 'a,b', 'c,\'d', 'g,''h')
    CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci' NOT NULL DEFAULT 'station';

INSERT INTO test_stations_10 ( `name`, `type` ) values ( 'Ha Tinh 1', 'now' );
