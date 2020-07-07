CREATE TABLE connector_test.business_order (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT '订单号',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT '订单号索引',
  KEY `merchant_id_index` (`merchant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='订单表';

CREATE TABLE connector_test.business_order_detail (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `return_product_num` int(11) DEFAULT NULL COMMENT '退货商品数量',
  `actual_price` decimal(10,2) DEFAULT '0.00' COMMENT '实际发货价格',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2572662 DEFAULT CHARSET=utf8 COMMENT='订单明细表';


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
       ) '实际金额小计(不含退货)'
FROM business_order bo
LEFT JOIN business_order_detail bod on bo.id=bod.order_id
;





