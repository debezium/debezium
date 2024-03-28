CREATE TABLE connector_test.business_order (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT 'order code',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT 'order index',
  KEY `merchant_id_index` (`merchant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='order table';

CREATE TABLE connector_test.business_order_detail (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `return_product_num` int(11) DEFAULT NULL COMMENT 'return product',
  `actual_price` decimal(10,2) DEFAULT '0.00' COMMENT 'actual price',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2572662 DEFAULT CHARSET=utf8 COMMENT='order detail';


CREATE DEFINER=`root`@`%` PROCEDURE `_Navicat_Temp_Stored_Proc`()
BEGIN CREATE TEMPORARY TABLE temp1
SELECT
    SUM(round(case
                   when
                        bod.return_product_num is not null
                   then
                        bod.actual_price-(CAST(bod.return_product_num AS DECIMAL(10))*unit_price)
                   else
                        bod.actual_price END,2)
       ) 'actual price'
FROM business_order bo
LEFT JOIN business_order_detail bod on bo.id=bod.order_id
;





