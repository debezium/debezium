-- debezium/dbz#360: aggregate function names used as column names
ALTER TABLE favourites CHANGE order_id sum int(11) DEFAULT NULL;
CREATE TABLE t1 (sum INT, count INT, avg INT, min INT, max INT);
