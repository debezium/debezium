CREATE TABLE connector_test.business_order (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT '订单号',
  `waterid` int(10) DEFAULT NULL COMMENT '库存系统的流水号',
  `merchant_id` int(11) NOT NULL COMMENT '商户id',
  `salesman_id` int(11) NOT NULL COMMENT '业务人员id',
  `status` tinyint(2) NOT NULL COMMENT '订单状态 5:生成，处理中 10:等待付款 15:订单确认 20:订单分拣中 25:物流配送中 30:订单完成 35:订单取消 40:已退款',
  `source` tinyint(4) NOT NULL DEFAULT '0' COMMENT '来源 0：tob 1：零售',
  `payment_status` int(11) NOT NULL COMMENT '支付状态：0:等待付款  1:已支付  2:欠款 | 默认:0',
  `price` decimal(10,2) NOT NULL COMMENT '订单价格',
  `actual_price` decimal(10,2) DEFAULT NULL COMMENT '实际发货价格',
  `refund_price` decimal(10,2) DEFAULT NULL COMMENT '退款金额',
  `payment_amount` decimal(10,2) NOT NULL COMMENT '支付金额',
  `product_num` int(4) NOT NULL COMMENT '商品数量',
  `actual_product_num` int(4) DEFAULT NULL COMMENT '实际分拣数量',
  `receiver_time` datetime NOT NULL COMMENT '收货时间',
  `receiver_time_slot` int(11) DEFAULT NULL COMMENT '收获时间段 1:上午,2:下午,3:晚上',
  `market_factor` decimal(5,2) DEFAULT NULL COMMENT '市场系数',
  `receiver` varchar(64) NOT NULL COMMENT '收货人',
  `phonenum` varchar(64) NOT NULL COMMENT '收货人手机号',
  `payment_type` tinyint(2) DEFAULT NULL COMMENT '支付方式 1:余额  5:货到付款 10:在线支付',
  `order_remark` varchar(256) DEFAULT NULL COMMENT '订单备注',
  `fail_reason` varchar(50) DEFAULT NULL COMMENT '失败原因',
  `fail_reason_detail` varchar(300) DEFAULT NULL COMMENT '失败详情原因',
  `uuid` varchar(45) DEFAULT NULL COMMENT '全局唯一id',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `created_by` varchar(64) DEFAULT NULL COMMENT '创建人username',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `updated_by` varchar(64) DEFAULT NULL COMMENT '更新人username',
  `order_type` int(1) NOT NULL DEFAULT '1' COMMENT '订单类型 1:用户下单  2：人工补单 ',
  `manual_reason` int(2) DEFAULT NULL COMMENT '补单原因 1:缺货断货，2：用户漏下单',
  `manual_reason_detail` varchar(512) DEFAULT NULL COMMENT '补单原因详细',
  `manual_service_fee` decimal(10,2) DEFAULT NULL COMMENT '服务费',
  `order_type_flag` tinyint(4) DEFAULT '1' COMMENT '订单类型标记 参考：OrderTypeFlag',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT '订单号索引',
  KEY `merchant_id_index` (`merchant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='订单表';

CREATE TABLE connector_test.business_order_detail (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `order_id` int(11) NOT NULL COMMENT '订单id',
  `merchant_id` int(11) NOT NULL COMMENT '商户id',
  `salesman_id` int(11) NOT NULL COMMENT '业务人员id',
  `product_id` int(11) NOT NULL COMMENT '商品id',
  `product_type` int(2) DEFAULT NULL COMMENT '商品类型 1:标品，0：非标品',
  `market_factor` decimal(5,2) DEFAULT NULL COMMENT '市场系数',
  `unit_price` decimal(10,2) NOT NULL COMMENT '商品单价',
  `price` decimal(10,2) NOT NULL COMMENT '合计价格',
  `actual_price` decimal(10,2) DEFAULT '0.00' COMMENT '实际发货价格',
  `product_num` int(4) NOT NULL COMMENT '商品数量',
  `product_weight` decimal(10,0) DEFAULT NULL COMMENT '商品重量',
  `actual_product_num` int(11) DEFAULT NULL COMMENT '实际商品数量',
  `actual_product_weight` decimal(10,0) DEFAULT NULL COMMENT '实际商品重量',
  `return_product_num` int(11) DEFAULT NULL COMMENT '退货商品数量',
  `return_product_weight` decimal(10,0) DEFAULT NULL COMMENT '返回商品重量',
  `return_price` decimal(10,2) DEFAULT NULL COMMENT '退货价格',
  `return_reason` varchar(256) DEFAULT NULL COMMENT '退货原因',
  `remark` varchar(1024) DEFAULT NULL COMMENT '描述',
  `status` tinyint(4) DEFAULT '1' COMMENT '状态',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `created_by` varchar(64) DEFAULT NULL COMMENT '创建人username',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `updated_by` varchar(64) DEFAULT NULL COMMENT '更新人username',
  `sys_no` varchar(64) DEFAULT NULL,
  `sku` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `INDEX_ORDER_PRODUCT` (`order_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2572662 DEFAULT CHARSET=utf8 COMMENT='订单明细表';


CREATE DEFINER=`root`@`%` PROCEDURE `_Navicat_Temp_Stored_Proc`(st datetime,et datetime,saleman varchar(50))
BEGIN CREATE TEMPORARY TABLE temp1
SELECT
    SUM(round(case
                   when
                        bod.return_product_weight is not null
                   then
                        bod.actual_price-floor(floor(bod.return_product_weight/50)/10*bod.unit_price*100)/100
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





