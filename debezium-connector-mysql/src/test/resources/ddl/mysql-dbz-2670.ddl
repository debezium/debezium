
CREATE TABLE connector_test.business_order_1 (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT 'order code',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT 'order index'
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='order table';

CREATE TABLE connector_test.business_order_2 (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL COMMENT 'order code',
  PRIMARY KEY (`id`),
  KEY `order_code_index` (`code`) COMMENT 'order index'
) ENGINE=InnoDB AUTO_INCREMENT=2000022760 DEFAULT CHARSET=utf8 COMMENT='order table';


CREATE TEMPORARY TABLE connector_test.temp1
SELECT
       bo.id 'id',
       bo.code '测试'
from
       business_order_1 bo
;

CREATE TEMPORARY TABLE connector_test.temp2
SELECT
       bo.id 'id',
       bo.code '测试'
from
       business_order_2 bo
;

SELECT
    t1.测试
FROM temp1 t1
LEFT JOIN
     temp2 t2 on t1.id = t2.id;
