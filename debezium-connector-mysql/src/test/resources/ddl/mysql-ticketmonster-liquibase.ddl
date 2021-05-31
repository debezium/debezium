DROP TABLE IF EXISTS ticketmonster.Booking;
DROP TABLE IF EXISTS ticketmonster.Appearance;
DROP TABLE IF EXISTS ticketmonster.Section;
DROP TABLE IF EXISTS ticketmonster.TicketCategory;
DROP TABLE IF EXISTS ticketmonster.TicketPriceGuide;
DROP TABLE IF EXISTS ticketmonster.SectionAllocation;
DROP TABLE IF EXISTS ticketmonster.Ticket;
DROP DATABASE IF EXISTS ticketmonster;
CREATE DATABASE ticketmonster;
USE ticketmonster;
CREATE TABLE `Appearance` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) DEFAULT NULL,
  `event_name` varchar(255) DEFAULT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `venue_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKb2ol0eoqtadvfoxhsnqcajgqa` (`event_id`,`venue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1;
CREATE TABLE `Booking` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `cancellationCode` varchar(255) NOT NULL,
  `contactEmail` varchar(255) NOT NULL,
  `createdOn` datetime(6) NOT NULL,
  `performance_id` bigint(20) DEFAULT NULL,
  `performance_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1;
CREATE TABLE `Section` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `numberOfRows` int(11) NOT NULL,
  `rowCapacity` int(11) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `venue_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKruosqireipse41rdsuvhqj050` (`name`,`venue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=latin1;
CREATE TABLE `SectionAllocation` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `allocated` longblob,
  `occupiedCount` int(11) NOT NULL,
  `performance_id` bigint(20) DEFAULT NULL,
  `performance_name` varchar(255) DEFAULT NULL,
  `version` bigint(20) NOT NULL,
  `section_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK25wlm457x8dmc00we5uw7an3s` (`performance_id`,`section_id`),
  KEY `FK60388cvbhb1xyrdhhe546t6dl` (`section_id`),
  CONSTRAINT `FK60388cvbhb1xyrdhhe546t6dl` FOREIGN KEY (`section_id`) REFERENCES `Section` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=57 DEFAULT CHARSET=latin1;
CREATE TABLE `Ticket` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `price` float NOT NULL,
  `number` int(11) NOT NULL,
  `rowNumber` int(11) NOT NULL,
  `section_id` bigint(20) DEFAULT NULL,
  `ticketCategory_id` bigint(20) NOT NULL,
  `tickets_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK7xoel6i5b4nrphore8ns2jtld` (`section_id`),
  KEY `FK88jejylfnpfqcslai19n4naqf` (`ticketCategory_id`),
  KEY `FKolbt9u28gyshci6ek9ep0rl5d` (`tickets_id`),
  CONSTRAINT `FK7xoel6i5b4nrphore8ns2jtld` FOREIGN KEY (`section_id`) REFERENCES `Section` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FK88jejylfnpfqcslai19n4naqf` FOREIGN KEY (`ticketCategory_id`) REFERENCES `TicketCategory` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FKolbt9u28gyshci6ek9ep0rl5d` FOREIGN KEY (`tickets_id`) REFERENCES `Booking` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=latin1;
CREATE TABLE `TicketCategory` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_43455ipnchbn6r4bg8pviai3g` (`description`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;
CREATE TABLE `TicketPriceGuide` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `price` float NOT NULL,
  `section_id` bigint(20) NOT NULL,
  `show_id` bigint(20) NOT NULL,
  `ticketCategory_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UKro227lwq9ma9gy3ik6gl27xgm` (`section_id`,`show_id`,`ticketCategory_id`),
  KEY `FK2nddwnrovke2wgpb8ffahqw` (`show_id`),
  KEY `FK3d06sbv9l20tk2wa6yjsw9xdd` (`ticketCategory_id`),
  CONSTRAINT `FK2nddwnrovke2wgpb8ffahqw` FOREIGN KEY (`show_id`) REFERENCES `Appearance` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FK3d06sbv9l20tk2wa6yjsw9xdd` FOREIGN KEY (`ticketCategory_id`) REFERENCES `TicketCategory` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FKaqmyqif55ipri4x65o8syt85k` FOREIGN KEY (`section_id`) REFERENCES `Section` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=latin1 