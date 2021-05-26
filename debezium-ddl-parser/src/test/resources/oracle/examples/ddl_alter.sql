-- Alter table partitioning
ALTER TABLE sales add partition p6 values less than (1996);
ALTER TABLE products add partition;
ALTER TABLE products add partition p5 tablespace u5;
ALTER TABLE customers add partition central_india values ('BHOPAL','NAGPUR');
ALTER TABLE products coalesce partition;
ALTER TABLE sales drop partition p5;
ALTER TABLE sales DROP PARTITION p5 UPDATE GLOBAL INDEXES;
ALTER TABLE sales merge partition p2 and p3 into partition p23;
ALTER TABLE customers MODIFY PARTITION south_india ADD VALUES ('KOCHI', 'MANGALORE');
ALTER TABLE customers MODIFY PARTITION south_india DROP VALUES ('KOCHI','MANGALORE');
ALTER TABLE sales split partition p5 into (Partition p6 values less than (1996), Partition p7 values less than (MAXVALUE));
ALTER TABLE sales truncate partition p5;
