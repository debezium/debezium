CREATE TABLE PEOPLE(name VARCHAR2(10));
ALTER TABLE PEOPLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLE debezium_table1 (id NUMERIC(9,0) NOT NULL, name VARCHAR2(1000), PRIMARY KEY (id));
ALTER TABLE debezium_table1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLE debezium_table2 (id NUMERIC(9,0) NOT NULL, name VARCHAR2(1000), PRIMARY KEY (id));
ALTER TABLE debezium_table2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLE debezium_table3 (id NUMERIC(9,0) NOT NULL, name VARCHAR2(1000), birth_date date, PRIMARY KEY (id));
ALTER TABLE debezium_table3 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLE TEST
(COLUMN1 NUMBER(19) NOT NULL,
COLUMN2 VARCHAR2(255),
PRIMARY KEY (COLUMN1));
ALTER TABLE TEST ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLE DEBEZIUM_TEST
(id NUMBER(19) NOT NULL,
col1 NUMERIC(4,2),
col2 VARCHAR2(255) DEFAULT 'debezium' NOT NULL,
col3 NVARCHAR2(255) NOT NULL,
col4 CHAR(4),
col5 NCHAR(4),
col6 FLOAT(126),
col7 DATE,
col8 TIMESTAMP,
col9 blob,
col10 clob,
col12 NUMBER(1,0),
col13 DATE NOT NULL,
PRIMARY KEY (id));
ALTER TABLE DEBEZIUM_TEST ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO PEOPLE (name) VALUES ('Larry');
INSERT INTO PEOPLE (name) VALUES ('Bruno');
INSERT INTO PEOPLE (name) VALUES ('Gerald');

CREATE TABLE products (
  id NUMBER(4) GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 101) NOT NULL PRIMARY KEY,
  name VARCHAR2(255) NOT NULL,
  description VARCHAR2(512),
  weight FLOAT
);
ALTER TABLE products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO products
  VALUES (NULL,'scooter','Small 2-wheel scooter',3.14);
INSERT INTO products
  VALUES (NULL,'car battery','12V car battery',8.1);
INSERT INTO products
  VALUES (NULL,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8);
INSERT INTO products
  VALUES (NULL,'hammer','12oz carpenter''s hammer',0.75);
INSERT INTO products
  VALUES (NULL,'hammer','14oz carpenter''s hammer',0.875);
INSERT INTO products
  VALUES (NULL,'hammer','16oz carpenter''s hammer',1.0);
INSERT INTO products
  VALUES (NULL,'rocks','box of assorted rocks',5.3);
INSERT INTO products
  VALUES (NULL,'jacket','water resistent black wind breaker',0.1);
INSERT INTO products
  VALUES (NULL,'spare tire','24 inch spare tire',22.2);

CREATE TABLE products_on_hand (
  product_id NUMBER(4) NOT NULL PRIMARY KEY,
  quantity NUMBER(4) NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);
ALTER TABLE products_on_hand ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO products_on_hand VALUES (101, 3);
INSERT INTO products_on_hand VALUES (102, 8);
INSERT INTO products_on_hand VALUES (103, 18);
INSERT INTO products_on_hand VALUES (104, 4);
INSERT INTO products_on_hand VALUES (105, 5);
INSERT INTO products_on_hand VALUES (106, 0);
INSERT INTO products_on_hand VALUES (107, 44);
INSERT INTO products_on_hand VALUES (108, 2);
INSERT INTO products_on_hand VALUES (109, 5);

CREATE TABLE customers (
  id NUMBER(4) GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 1001) NOT NULL PRIMARY KEY,
  first_name VARCHAR2(255) NOT NULL,
  last_name VARCHAR2(255) NOT NULL,
  email VARCHAR2(255) NOT NULL UNIQUE
);
ALTER TABLE customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO customers
  VALUES (NULL,'Sally','Thomas','sally.thomas@acme.com');
INSERT INTO customers
  VALUES (NULL,'George','Bailey','gbailey@foobar.com');
INSERT INTO customers
  VALUES (NULL,'Edward','Walker','ed@walker.com');
INSERT INTO customers
  VALUES (NULL,'Anne','Kretchmar','annek@noanswer.org');

CREATE TABLE debezium.orders (
  id NUMBER(6) GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 10001) NOT NULL PRIMARY KEY,
  order_date DATE NOT NULL,
  purchaser NUMBER(4) NOT NULL,
  quantity NUMBER(4) NOT NULL,
  product_id NUMBER(4) NOT NULL,
  FOREIGN KEY (purchaser) REFERENCES customers(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);
ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO orders
  VALUES (NULL, '16-JAN-2016', 1001, 1, 102);
INSERT INTO orders
  VALUES (NULL, '17-JAN-2016', 1002, 2, 105);
INSERT INTO orders
  VALUES (NULL, '19-FEB-2016', 1002, 2, 106);
INSERT INTO orders
  VALUES (NULL, '21-FEB-2016', 1003, 1, 107);

COMMIT;
