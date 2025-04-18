--
-- Contains statements with quoted identifiers
--
DROP TABLE IF EXISTS connector_test_ro.products_on_hand;
DROP TABLE IF EXISTS connector_test_ro.customers;
DROP TABLE IF EXISTS connector_test_ro.orders;
DROP TABLE IF EXISTS connector_test_ro.products;
DROP DATABASE IF EXISTS connector_test_ro;
CREATE DATABASE `connector_test_ro`;
USE `connector_test_ro`;
CREATE TABLE `customers` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(255) NOT NULL,
  `last_name` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=1005 DEFAULT CHARSET=latin1;
CREATE TABLE `orders` (
  `order_number` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` date NOT NULL,
  `purchaser` int(11) NOT NULL,
  `quantity` int(11) NOT NULL,
  `product_id` int(11) NOT NULL,
  PRIMARY KEY (`order_number`),
  KEY `order_customer` (`purchaser`),
  KEY `ordered_product` (`product_id`),
  CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`purchaser`) REFERENCES `customers` (`id`),
  CONSTRAINT `orders_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10006 DEFAULT CHARSET=latin1;
CREATE TABLE `products` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `description` varchar(512) DEFAULT NULL,
  `weight` float DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=latin1;
CREATE TABLE `products_on_hand` (
  `product_id` int(11) NOT NULL,
  `quantity` int(11) NOT NULL,
  PRIMARY KEY (`product_id`),
  CONSTRAINT `products_on_hand_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1